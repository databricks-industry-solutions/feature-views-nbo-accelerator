import { describe, expect, it } from 'vitest';
import { catchUpBatch, holdPaused, isPaused, pause, powerStatus, resume, startPower, tick } from './power';

const DAY = 24 * 60 * 60_000;

// A fake workspace: a Lakebase endpoint whose PATCH flips spec.disabled (a disabled compute reports IDLE and refuses
// connections), one live feature with a scheduled batch materialization job, and that job's run history.
function workspace(disabled = false, state = 'ACTIVE', lastSuccessMs: number | null = Date.now() - 60_000) {
  const ep = { disabled, state };
  const schedule = { quartz_cron_expression: '0 0 2 * * ?', timezone_id: 'UTC', pause_status: 'UNPAUSED' };
  const calls: string[] = [];
  const runs: { run_id: number; end_time?: number; state: { life_cycle_state: string; result_state?: string } }[] =
    lastSuccessMs === null ? [] : [{ run_id: 1, end_time: lastSuccessMs, state: { life_cycle_state: 'TERMINATED', result_state: 'SUCCESS' } }];
  const api = async <T,>(method: string, path: string, body?: unknown): Promise<T> => {
    if (method !== 'GET') calls.push(`${method} ${path.split('?')[0]}`);
    if (path.startsWith('/api/2.0/postgres/')) {
      if (method === 'PATCH') {
        ep.disabled = (body as { spec: { disabled: boolean } }).spec.disabled;
        ep.state = ep.disabled ? 'IDLE' : 'ACTIVE';
      }
      return { status: { current_state: ep.state, disabled: ep.disabled } } as T;
    }
    if (path.includes('materialized-features')) {
      return { materialized_features: [
        { job_id: 77, streaming_mode: { mode: 'RTM' } }, // the streaming leg: not a batch job
        { job_id: 55, cron_schedule_trigger: { cron_expression: '0 0 2 * * ?' } },
      ] } as T;
    }
    if (path.includes('/jobs/runs/list')) {
      return { runs: path.includes('active_only=true') ? [] : runs } as T;
    }
    if (path === '/api/2.1/jobs/run-now') {
      runs.unshift({ run_id: 2, end_time: Date.now(), state: { life_cycle_state: 'TERMINATED', result_state: 'SUCCESS' } });
      return { run_id: 2 } as T;
    }
    if (path.startsWith('/api/2.1/jobs/get')) return { settings: { schedule: { ...schedule } } } as T;
    if (path === '/api/2.1/jobs/update') {
      Object.assign(schedule, (body as { new_settings: { schedule: typeof schedule } }).new_settings.schedule);
      return {} as T;
    }
    if (path.includes('/jobs/runs/get')) return runs.find((r) => path.endsWith(`=${r.run_id}`)) as T;
    throw new Error(`unexpected ${method} ${path}`);
  };
  const wake = async () => {
    if (ep.disabled) throw new Error('connection refused');
  };
  return { ep, schedule, calls, api, wake };
}
const start = (ws: ReturnType<typeof workspace>) =>
  startPower({ api: ws.api, endpoint: 'projects/p/branches/b/endpoints/e', wake: ws.wake, features: ['c.s.f'] }, 0);

describe('demo power', () => {
  it('starts paused when the compute is already disabled (app restart while paused)', async () => {
    const ws = workspace(true, 'IDLE');
    await start(ws);
    expect(isPaused()).toBe(true);
  });

  it('pauses on Turn off and wakes on Warm up', async () => {
    const ws = workspace();
    await start(ws);
    expect(isPaused()).toBe(false);
    holdPaused();
    expect(isPaused()).toBe(true); // warm calls stop at once
    await pause();
    expect(ws.ep.disabled).toBe(true);
    expect(ws.schedule.pause_status).toBe('PAUSED'); // the batch job's schedule pauses with the demo
    expect(ws.calls).toEqual(['PATCH /api/2.0/postgres/projects/p/branches/b/endpoints/e', 'POST /api/2.1/jobs/update']);
    await resume();
    await catchUpBatch();
    expect(ws.schedule.pause_status).toBe('UNPAUSED');
    expect(ws.ep.disabled).toBe(false);
    expect(powerStatus()).toMatchObject({ paused: false, disabled: false, lakebase: 'ACTIVE', step: null, error: null });
  });

  it('runs the batch job before the pipelines start only when its last success is over a day old', async () => {
    const fresh = workspace();
    await start(fresh);
    await catchUpBatch();
    expect(fresh.calls).toEqual([]); // succeeded a minute ago, and its schedule is already on

    const stale = workspace(false, 'ACTIVE', Date.now() - 2 * DAY);
    await start(stale);
    await catchUpBatch();
    expect(stale.calls).toEqual(['POST /api/2.1/jobs/run-now']); // only the batch job, not the streaming one

    const never = workspace(false, 'ACTIVE', null);
    await start(never);
    await catchUpBatch();
    expect(never.calls).toEqual(['POST /api/2.1/jobs/run-now']);
  });

  it('re-disables a compute that is enabled and idle while paused', async () => {
    const ws = workspace();
    await start(ws);
    holdPaused();
    ws.ep.state = 'IDLE';
    await tick();
    expect(ws.ep.disabled).toBe(true);
  });
});
