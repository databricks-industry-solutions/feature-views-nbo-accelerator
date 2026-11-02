import { describe, expect, it } from 'vitest';
import { phaseOf, pipelinesRunning, pipelineStatus, startPipelines, turnOff, warmUp, type Pipe } from './pipelines';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const pipe = (id: string, role: Pipe['role'], state: string, update: string | null = null): Pipe => ({ id, label: id, role, state, update });

// A fake workspace: one Stream, two streaming features, and pipelines that start and stop on request.
function workspace() {
  const state: Record<string, { state: string; update: string }> = {
    ing: { state: 'IDLE', update: 'CANCELED' }, rtm1: { state: 'IDLE', update: 'CANCELED' }, rtm2: { state: 'RUNNING', update: 'RUNNING' },
  };
  const calls: string[] = [];
  const api = async <T,>(method: string, path: string): Promise<T> => {
    if (method !== 'GET') calls.push(`${method} ${path}`);
    if (path.startsWith('/api/2.0/feature-engineering/streams/')) return { ingestion_config: { ingestion_pipeline_id: 'ing' } } as T;
    if (path.includes('materialized-features')) {
      const id = path.includes('f1') ? 'rtm1' : 'rtm2';
      return { materialized_features: [{ pipeline_id: id, streaming_mode: { mode: 'RTM' } }, { pipeline_id: 'batch' }] } as T;
    }
    const m = path.match(/pipelines\/(\w+)(\/(updates|stop))?$/);
    const p = state[m![1]];
    if (m![3] === 'updates') Object.assign(p, { state: 'RUNNING', update: 'RUNNING' });
    if (m![3] === 'stop') Object.assign(p, { state: 'IDLE', update: 'CANCELED' });
    return { state: p.state, latest_updates: [{ state: p.update }] } as T;
  };
  return { state, calls, api };
}

describe('live pipelines', () => {
  it('derives the phase from the Real-Time Mode pipelines', () => {
    expect(phaseOf([], false)).toBe('unknown');
    expect(phaseOf([pipe('i', 'ingestion', 'IDLE'), pipe('r', 'rtm', 'RUNNING', 'RUNNING')], false)).toBe('ready');
    expect(phaseOf([pipe('r', 'rtm', 'RUNNING', 'INITIALIZING')], false)).toBe('starting');
    expect(phaseOf([pipe('r', 'rtm', 'IDLE', 'CANCELED')], true)).toBe('starting');
    expect(phaseOf([pipe('r', 'rtm', 'STOPPING', 'RUNNING')], false)).toBe('stopping');
    expect(phaseOf([pipe('r', 'rtm', 'RUNNING', 'STOPPING')], true)).toBe('stopping');
    expect(phaseOf([pipe('r', 'rtm', 'IDLE', 'CANCELED'), pipe('s', 'rtm', 'RUNNING', 'RUNNING')], false)).toBe('off');
  });

  it('warms up only stopped pipelines and keeps them on until Turn off', async () => {
    const ws = workspace();
    startPipelines({ api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'] });

    const up = await warmUp();
    expect(ws.calls.sort()).toEqual(['POST /api/2.0/pipelines/ing/updates', 'POST /api/2.0/pipelines/rtm1/updates']);
    expect(up.phase).toBe('ready');
    expect(up.pipes.map((p) => p.id).sort()).toEqual(['ing', 'rtm1', 'rtm2']); // the batch pipeline is not live

    await sleep(300);
    expect(ws.state.rtm1.state).toBe('RUNNING'); // nothing turns them off on its own

    const off = await turnOff();
    expect(['off', 'stopping']).toContain(off.phase);
    expect(Object.values(ws.state).every((p) => p.state === 'IDLE')).toBe(true);
  });

  it('says ready only after three fast test events in a row', async () => {
    await turnOff();
    const ws = workspace();
    const times = [21_000, 400, 1_500, 300, 250, 200];
    startPipelines({ api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'], probe: async () => times.shift() ?? 100 });
    await warmUp();
    const phases: string[] = [];
    for (let i = 0; i < 10; i += 1) {
      phases.push(pipelineStatus().phase);
      await sleep(10);
    }
    expect(phases.slice(0, 5).every((p) => p === 'starting')).toBe(true);
    expect(phases[phases.length - 1]).toBe('ready');
    expect(times).toEqual([]); // 21 s, 0.4, 1.5 (resets), then 0.3, 0.25, 0.2
    await turnOff();
  });

  it('queues Warm up while pipelines are still stopping', async () => {
    await turnOff();
    const ws = workspace();
    Object.assign(ws.state.rtm1, { state: 'RUNNING', update: 'STOPPING' });
    startPipelines({ api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'] });
    const st = await warmUp();
    expect(st).toMatchObject({ phase: 'starting', queued: true });
    expect(ws.calls).toEqual([]);
    Object.assign(ws.state.rtm1, { state: 'IDLE', update: 'CANCELED' });
    await sleep(4_200); // next status read sees it stopped
    pipelineStatus();
    await sleep(50);
    pipelineStatus();
    await sleep(50);
    expect(ws.calls).toContain('POST /api/2.0/pipelines/rtm1/updates');
    expect(pipelineStatus().queued).toBe(false);
    await turnOff();
  }, 10_000);

  it('reports running pipelines to the Bank tab without waiting for the probe', async () => {
    await turnOff();
    const ws = workspace();
    startPipelines({ api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'], probe: async () => 21_000 });
    await warmUp();
    expect(pipelineStatus().phase).toBe('starting'); // the probe has not passed
    expect(pipelinesRunning()).toBe(true);
    await turnOff();
    expect(pipelinesRunning()).toBe(false);
  });
});
