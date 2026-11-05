import { describe, expect, it } from 'vitest';
import { phaseOf, pipelinesRunning, pipelineStatus, startPipelines, turnOff, warmUp, watch, type Pipe } from './pipelines';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const pipe = (id: string, role: Pipe['role'], state: string, update: string | null = null): Pipe => ({ id, label: id, role, state, update });

// A fake workspace: one Stream, two streaming features, and pipelines that start and stop on request.
function workspace() {
  const state: Record<string, { state: string; update: string }> = {
    ing: { state: 'IDLE', update: 'CANCELED' }, rtm1: { state: 'IDLE', update: 'CANCELED' }, rtm2: { state: 'RUNNING', update: 'RUNNING' },
  };
  const calls: string[] = [];
  const api = async <T,>(method: string, path: string, _body?: unknown): Promise<T> => {
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

  it('wakes the online store before starting, and pauses it once every pipeline is down', async () => {
    await turnOff();
    const ws = workspace();
    const order: string[] = [];
    startPipelines({
      api: async <T,>(m: string, p: string, b?: unknown) => { if (m !== 'GET') order.push(p.split('/').pop()!); return ws.api<T>(m, p, b); },
      stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'],
      prepare: async () => { order.push('prepare'); await sleep(50); },
      whenStopped: async () => { order.push('whenStopped'); },
    }, 0);
    const st = await warmUp();
    expect(st.phase).toBe('starting'); // still waking the store
    expect(ws.calls).toEqual([]);
    await sleep(120);
    expect(order[0]).toBe('prepare');
    expect(order.slice(1).sort()).toEqual(['updates', 'updates']); // ing + rtm1, only after prepare
    await turnOff();
    expect(order[order.length - 1]).toBe('whenStopped');
  });

  it('Turn off during a slow prepare keeps the pipelines from starting afterwards', async () => {
    await turnOff();
    const ws = workspace();
    Object.assign(ws.state.rtm2, { state: 'IDLE', update: 'CANCELED' });
    let stopped = false;
    startPipelines({
      api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'],
      prepare: async () => { await sleep(100); }, // e.g. the batch job catching up
      whenStopped: async () => { stopped = true; },
    }, 0);
    await warmUp();
    await turnOff();
    await sleep(150);
    expect(ws.calls.filter((c) => c.endsWith('/updates'))).toEqual([]);
    await watch(); // the next pass finishes the pending Turn off
    expect(stopped).toBe(true);
  });

  it('watchdog stops ingestion that runs with no Real-Time Mode pipeline', async () => {
    await turnOff();
    const ws = workspace();
    Object.assign(ws.state.rtm2, { state: 'IDLE', update: 'CANCELED' });
    startPipelines({ api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'] }, 0);
    Object.assign(ws.state.ing, { state: 'RUNNING', update: 'RUNNING' }); // Feature Engineering restarted it
    await watch();
    expect(ws.calls).toEqual(['POST /api/2.0/pipelines/ing/stop']);
    expect(ws.state.ing.state).toBe('IDLE');

    // With a Real-Time Mode pipeline up, ingestion is needed and stays on.
    Object.assign(ws.state.ing, { state: 'RUNNING', update: 'RUNNING' });
    Object.assign(ws.state.rtm1, { state: 'RUNNING', update: 'RUNNING' });
    await watch();
    expect(ws.calls).toHaveLength(1);
    await turnOff();
  });

  it('watchdog leaves a fresh Warm up alone', async () => {
    await turnOff();
    const ws = workspace();
    startPipelines({ api: ws.api, stream: 'c.s.stream', features: ['c.s.f1', 'c.s.f2'] }, 0);
    await warmUp();
    Object.assign(ws.state.rtm1, { state: 'IDLE', update: 'CANCELED' });
    Object.assign(ws.state.rtm2, { state: 'IDLE', update: 'CANCELED' });
    ws.calls.length = 0;
    await watch();
    expect(ws.calls).toEqual([]); // within the grace period after Warm up
    await turnOff();
  });

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
