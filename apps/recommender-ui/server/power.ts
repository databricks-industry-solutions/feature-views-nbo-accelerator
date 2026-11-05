// ---------------------------------------------------------------------------
// Demo power: Turn off pipelines also pauses the Lakebase online store and stops the app's warm calls to the
// serving endpoints, so they scale to zero. Warm up wakes Lakebase before the pipelines start (the Real-Time Mode
// pipelines write to it) and warms both endpoints. Paused means the Lakebase compute is disabled.
//
// The scheduled batch materialization job writes to Lakebase too, so Turn off also pauses its schedule. Warm up
// resumes the schedule and runs the job once before the pipelines start when it has not succeeded in the last
// BATCH_FRESH_MS: that refreshes the batch side of the Sawtooth features and the streaming table's day partitions.
// ---------------------------------------------------------------------------

export interface PowerDeps {
  api: <T>(method: string, path: string, body?: unknown) => Promise<T>; // app service principal
  endpoint: string; // projects/<online store>/branches/production/endpoints/primary
  wake: () => Promise<void>; // opens a Lakebase connection; throws until the compute accepts one
  features?: string[]; // full names of the live features; their scheduled batch materialization jobs catch up
}
export interface PowerStatus {
  paused: boolean;
  lakebase: string | null; // current_state of the compute, e.g. ACTIVE or IDLE
  disabled: boolean | null;
  step: string | null; // what Warm up or Turn off is doing with Lakebase right now
  error: string | null;
}

const TICK_MS = 60_000;
const SETTLE_MS = 3 * 60_000; // how long a disable/enable or the first connection may take
const BATCH_FRESH_MS = 24 * 60 * 60_000;
const BATCH_TIMEOUT_MS = 30 * 60_000;
const BATCH_POLL_MS = 10_000;

let deps: PowerDeps | null = null;
let paused = false;
let lb: { state: string | null; disabled: boolean } | null = null;
let step: string | null = null;
let error: string | null = null;
let busy = false;
let timer: NodeJS.Timeout | null = null;
// Resolves once startPower has read the compute, so code that runs before startPower can still wait for it.
let markReady: () => void = () => undefined;
const ready = new Promise<void>((r) => { markReady = r; });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function read(d: PowerDeps) {
  const ep = await d.api<{ status?: { current_state?: string; disabled?: boolean } }>('GET', `/api/2.0/postgres/${d.endpoint}`);
  lb = { state: ep.status?.current_state ?? null, disabled: !!ep.status?.disabled };
}

async function setDisabled(d: PowerDeps, disabled: boolean) {
  await d.api('PATCH', `/api/2.0/postgres/${d.endpoint}?update_mask=spec.disabled`, { spec: { disabled } });
  const until = Date.now() + SETTLE_MS;
  for (;;) {
    await read(d);
    if (lb?.disabled === disabled) return;
    if (disabled && !paused) return; // Warm up was pressed while pausing; resume() turns it back on
    if (Date.now() > until) throw new Error(`Lakebase compute still ${disabled ? 'enabled' : 'disabled'} after ${SETTLE_MS / 60_000} min`);
    await sleep(2_000);
  }
}

/** Whether the demo is paused: the app sends no warm calls and the visitor pages say to press Warm up. */
export const isPaused = () => paused;
export const powerReady = () => ready;

export function powerStatus(): PowerStatus {
  return { paused, lakebase: lb?.state ?? null, disabled: lb?.disabled ?? null, step, error };
}

/** Turn off pressed: stop the warm calls now. Lakebase pauses once the pipelines have stopped (pause()). */
export function holdPaused() {
  if (deps) paused = true;
}

/** Disable the Lakebase compute. */
export async function pause(): Promise<void> {
  const d = deps;
  if (!d) return;
  paused = true;
  error = null;
  busy = true;
  step = 'Pausing Lakebase';
  try {
    await setDisabled(d, true);
    if (paused) await setBatchSchedules(d, 'PAUSED');
  } catch (e) {
    error = String(e).slice(0, 300);
    throw e;
  } finally {
    step = null;
    busy = false;
  }
}

/** Enable the Lakebase compute and wait until it accepts a connection. */
export async function resume(): Promise<void> {
  const d = deps;
  if (!d) return;
  paused = false;
  error = null;
  busy = true;
  step = 'Waking Lakebase';
  try {
    await read(d);
    if (lb?.disabled) await setDisabled(d, false);
    const until = Date.now() + SETTLE_MS;
    let last: unknown = null;
    while (Date.now() < until) {
      try {
        await d.wake();
        await read(d).catch(() => undefined);
        return;
      } catch (e) {
        last = e;
        await sleep(3_000);
      }
    }
    throw new Error(`Lakebase did not accept a connection within ${SETTLE_MS / 60_000} min: ${String(last).slice(0, 200)}`);
  } catch (e) {
    error = String(e).slice(0, 300);
    throw e;
  } finally {
    step = null;
    busy = false;
  }
}

type Run = { run_id: number; end_time?: number; state?: { life_cycle_state?: string; result_state?: string; state_message?: string } };

async function batchJobs(d: PowerDeps): Promise<number[]> {
  const jobs = new Set<number>();
  for (const feature of d.features ?? []) {
    const r = await d.api<{ materialized_features?: { job_id?: number; streaming_mode?: unknown; cron_schedule_trigger?: unknown }[] }>(
      'GET', `/api/2.0/feature-engineering/materialized-features?feature_name=${encodeURIComponent(feature)}`);
    for (const m of r.materialized_features ?? []) if (!m.streaming_mode && m.cron_schedule_trigger && m.job_id) jobs.add(m.job_id);
  }
  return [...jobs];
}

type Schedule = { quartz_cron_expression: string; timezone_id: string; pause_status?: string };

/** Pause or resume the schedule of every scheduled batch materialization job of the live features. */
async function setBatchSchedules(d: PowerDeps, status: 'PAUSED' | 'UNPAUSED') {
  for (const jobId of await batchJobs(d)) {
    const job = await d.api<{ settings?: { schedule?: Schedule } }>('GET', `/api/2.1/jobs/get?job_id=${jobId}`);
    const schedule = job.settings?.schedule;
    if (!schedule || schedule.pause_status === status) continue;
    await d.api('POST', '/api/2.1/jobs/update', { job_id: jobId, new_settings: { schedule: { ...schedule, pause_status: status } } });
    console.log(`[power] batch materialization job ${jobId} schedule ${status}`);
  }
}

async function waitForRun(d: PowerDeps, runId: number): Promise<Run> {
  const until = Date.now() + BATCH_TIMEOUT_MS;
  for (;;) {
    const run = await d.api<Run>('GET', `/api/2.1/jobs/runs/get?run_id=${runId}`);
    const lc = run.state?.life_cycle_state;
    if (lc === 'TERMINATED' || lc === 'SKIPPED' || lc === 'INTERNAL_ERROR') return run;
    if (Date.now() > until) throw new Error(`batch materialization run ${runId} still ${lc} after ${BATCH_TIMEOUT_MS / 60_000} min`);
    await sleep(BATCH_POLL_MS);
  }
}

/** Run each scheduled batch materialization job that has not succeeded in the last 24 hours (or wait for a run
 *  already in progress). Call after resume(), since the job writes to Lakebase. */
export async function catchUpBatch(): Promise<void> {
  const d = deps;
  if (!d) return;
  step = 'Checking the batch materialization job';
  try {
    await setBatchSchedules(d, 'UNPAUSED');
    for (const jobId of await batchJobs(d)) {
      const active = await d.api<{ runs?: Run[] }>('GET', `/api/2.1/jobs/runs/list?job_id=${jobId}&active_only=true&limit=1`);
      let runId = active.runs?.[0]?.run_id;
      if (!runId) {
        const done = await d.api<{ runs?: Run[] }>('GET', `/api/2.1/jobs/runs/list?job_id=${jobId}&completed_only=true&limit=25`);
        const ok = (done.runs ?? []).find((r) => r.state?.result_state === 'SUCCESS');
        if (ok?.end_time && Date.now() - ok.end_time < BATCH_FRESH_MS) continue;
        runId = (await d.api<{ run_id: number }>('POST', '/api/2.1/jobs/run-now', { job_id: jobId })).run_id;
        console.log(`[power] batch materialization job ${jobId} last succeeded ${ok?.end_time ? new Date(ok.end_time).toISOString() : 'never'}: started run ${runId}`);
      }
      step = 'Running the daily batch materialization job (about 7 minutes)';
      const run = await waitForRun(d, runId);
      if (run.state?.result_state !== 'SUCCESS') {
        throw new Error(`batch materialization run ${runId} ended ${run.state?.result_state ?? run.state?.life_cycle_state}: ${(run.state?.state_message ?? '').slice(0, 150)}`);
      }
    }
  } catch (e) {
    error = String(e).slice(0, 300);
    throw e;
  } finally {
    step = null;
  }
}

/** While paused, keep the compute disabled, e.g. after a failed pause. */
export async function tick(): Promise<void> {
  const d = deps;
  if (!d || busy) return;
  try {
    await read(d);
    if (!paused || !lb || lb.disabled || lb.state === 'ACTIVE') return;
    busy = true;
    console.log('[power] paused and Lakebase idle but enabled: disabling it');
    await setDisabled(d, true);
  } catch (e) {
    error = String(e).slice(0, 300);
  } finally {
    busy = false;
  }
}

export function startPower(d: PowerDeps, tickMs = TICK_MS) {
  deps = d;
  // A restart keeps the state: a disabled compute means the demo was paused.
  // The first read decides paused or not, so retry it a few times (credentials can lag at app start).
  const first = (async () => {
    for (let i = 1; ; i += 1) {
      try {
        await read(d);
        paused = !!lb?.disabled;
        error = null;
        return;
      } catch (e) {
        error = String(e).slice(0, 300);
        if (i >= 5) return;
        await sleep(3_000);
      }
    }
  })().finally(markReady);
  if (timer) clearInterval(timer);
  timer = tickMs > 0 ? setInterval(() => void tick(), tickMs) : null;
  timer?.unref();
  return first;
}
