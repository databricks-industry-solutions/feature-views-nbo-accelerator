// ---------------------------------------------------------------------------
// Live pipelines: the Kafka ingestion pipeline and the Real-Time Mode materialization pipelines behind the
// Live stream page. Together they are most of the demo's running cost, so they stay off until someone presses
// Warm up, and stay on until someone presses Turn off pipelines. Pipeline ids come from the Feature Engineering
// API, not config, because re-materializing a feature creates new pipelines.
// ---------------------------------------------------------------------------

export interface PipelineDeps {
  api: <T>(method: string, path: string, body?: unknown) => Promise<T>; // app service principal
  stream: string; // catalog.schema.stream_name
  features: string[]; // full names of the streaming features
  probe?: () => Promise<number>; // ms for one test event to become servable end to end
  // Warm up runs this before it starts any pipeline: the Real-Time Mode pipelines write to the online store, so
  // it has to be up first. Turn off runs whenStopped once every pipeline has stopped.
  prepare?: () => Promise<void>;
  whenStopped?: () => Promise<void>;
  // Turn off on its own after idleMs with no user activity, unless busy() (e.g. live traffic running).
  autoOff?: { idleMs: number; lastActivity: () => number; busy: () => boolean; before?: () => void };
}

export type Phase = 'checking' | 'off' | 'starting' | 'ready' | 'stopping' | 'unknown';
export interface Pipe { id: string; label: string; role: 'ingestion' | 'rtm'; state: string; update: string | null }
export interface PipelineStatus {
  phase: Phase;
  pipes: Pipe[];
  failed: string[];
  probe_ms: number | null; // last test event, while the running pipelines are still catching up
  queued: boolean; // Warm up was pressed while pipelines were still stopping; they start once stopped
  error: string | null;
  // null when auto-off is disabled. off_in_s: seconds until it turns the demo off, null while it would not (pipelines
  // off, traffic running, Warm up or Turn off in progress).
  auto_off: { idle_min: number; turned_off_at: number | null; off_in_s: number | null } | null;
}

const REFRESH_MS = 4_000;
const RESOLVE_MS = 5 * 60_000;
// Ready needs this many test events in a row servable within PROBE_OK_MS, not just updates in RUNNING.
const PROBE_OK_MS = 1_000;
const PROBES_NEEDED = 3;
const STARTING = new Set(['STARTING', 'DEPLOYING', 'RECOVERING', 'RESETTING']);
const ACTIVE = new Set(['RUNNING', 'STOPPING', ...STARTING]);
// Feature Engineering restarts a stopped ingestion pipeline every 2 hours. With every Real-Time Mode pipeline off,
// the watchdog stops it again; a Warm up in the last WARM_GRACE_MS is left alone.
const WATCH_MS = 60_000;
const WARM_GRACE_MS = 10 * 60_000;
// A stopped pipeline can still report state RUNNING while its update is stopping.
const STOPPING_UPDATE = new Set(['STOPPING', 'CANCELING']);

let deps: PipelineDeps | null = null;
let pipes: Pipe[] = [];
let resolved: { at: number; list: Omit<Pipe, 'state' | 'update'>[] } | null = null;
let refreshedAt = 0;
let loaded = false; // read at least once
let refreshing: Promise<void> | null = null;
let readError: string | null = null;
let actionError: string | null = null;
let requestedAt = 0; // last Warm up press; covers the seconds before the API reports the new update
let verified = false;
let probing = false;
let probeOk = 0;
let probeMs: number | null = null;
let queued = false;
let preparing = false; // Warm up is waiting on prepare() before it starts the pipelines
let cancelWarm = false; // Turn off was pressed while Warm up was still preparing
let pauseWhenStopped = false; // Turn off was pressed; run whenStopped once every pipeline is down
let watchTimer: NodeJS.Timeout | null = null;
let autoOffAt: number | null = null; // when the watchdog last turned the demo off for inactivity

// One test event per call, run while someone is watching the status (the Live stream page polls it).
function kickProbe(d: PipelineDeps) {
  if (probing || !d.probe) return;
  probing = true;
  d.probe().then((ms) => {
    probeMs = ms;
    probeOk = ms < PROBE_OK_MS ? probeOk + 1 : 0;
    if (probeOk >= PROBES_NEEDED) verified = true;
  }, () => { probeOk = 0; }).finally(() => { probing = false; });
}

async function resolvePipes(d: PipelineDeps) {
  if (resolved && Date.now() - resolved.at < RESOLVE_MS) return resolved.list;
  const list: Omit<Pipe, 'state' | 'update'>[] = [];
  const stream = await d.api<{ ingestion_config?: { ingestion_pipeline_id?: string } }>(
    'GET', `/api/2.0/feature-engineering/streams/${d.stream}`);
  const ingest = stream.ingestion_config?.ingestion_pipeline_id;
  if (ingest) list.push({ id: ingest, label: 'Kafka ingestion', role: 'ingestion' });
  for (const feature of d.features) {
    const r = await d.api<{ materialized_features?: { pipeline_id?: string; streaming_mode?: unknown }[] }>(
      'GET', `/api/2.0/feature-engineering/materialized-features?feature_name=${encodeURIComponent(feature)}`);
    for (const m of r.materialized_features ?? []) {
      if (m.streaming_mode && m.pipeline_id && !list.some((p) => p.id === m.pipeline_id)) {
        list.push({ id: m.pipeline_id, label: feature.split('.').pop() ?? feature, role: 'rtm' });
      }
    }
  }
  resolved = { at: Date.now(), list };
  return list;
}

async function refresh(): Promise<void> {
  const d = deps;
  if (!d) return;
  if (refreshing) return refreshing;
  refreshing = (async () => {
    const list = await resolvePipes(d);
    pipes = await Promise.all(list.map(async (p) => {
      const g = await d.api<{ state?: string; latest_updates?: { state?: string }[] }>('GET', `/api/2.0/pipelines/${p.id}`);
      return { ...p, state: g.state ?? 'UNKNOWN', update: g.latest_updates?.[0]?.state ?? null };
    }));
    refreshedAt = Date.now();
    loaded = true;
    readError = null;
  })().catch((e) => {
    readError = String(e).slice(0, 300);
    resolved = null; // a permission or id change shows up here; resolve again next time
  }).finally(() => { refreshing = null; });
  return refreshing;
}

const isUp = (p: Pipe) => p.state === 'RUNNING' && p.update === 'RUNNING';

export function phaseOf(list: Pipe[], requestedRecently: boolean): Phase {
  const rtm = list.filter((p) => p.role === 'rtm');
  if (!rtm.length) return 'unknown';
  if (rtm.every(isUp)) return 'ready';
  if (list.some((p) => p.state === 'STOPPING' || (p.update !== null && STOPPING_UPDATE.has(p.update)))) return 'stopping';
  if (list.some((p) => STARTING.has(p.state) || (p.state === 'RUNNING' && !isUp(p))) || requestedRecently) return 'starting';
  return 'off';
}

export function pipelineStatus(): PipelineStatus {
  if (Date.now() - refreshedAt > REFRESH_MS) void refresh();
  const now = Date.now();
  let phase: Phase = !loaded ? (readError ? 'unknown' : 'checking') : phaseOf(pipes, preparing || now - requestedAt < 30_000);
  if (queued && phase !== 'stopping' && Date.now() - refreshedAt < REFRESH_MS) {
    queued = false;
    warmUp().catch((e) => { actionError = String(e).slice(0, 300); });
    phase = 'starting';
  } else if (queued) {
    phase = 'starting';
  }
  if (phase !== 'ready') {
    verified = false;
    probeOk = 0;
    probeMs = null;
  } else if (deps?.probe && !verified) {
    kickProbe(deps);
    phase = 'starting';
  }
  return {
    phase,
    pipes,
    failed: pipes.filter((p) => p.state === 'FAILED').map((p) => p.label),
    probe_ms: phase === 'starting' ? probeMs : null,
    queued,
    error: actionError ?? readError,
    auto_off: autoOffStatus(),
  };
}

/** Whether the Real-Time Mode pipelines are running, without the freshness probe that gates Ready on the Live
 *  stream page. null until the first read. The Lakeshore Bank tab uses this, since it never polls the probe. */
export function pipelinesRunning(): boolean | null {
  if (Date.now() - refreshedAt > REFRESH_MS) void refresh();
  return loaded ? phaseOf(pipes, false) === 'ready' : null;
}

async function startStopped(d: PipelineDeps) {
  const results = await Promise.allSettled(pipes.filter((p) => p.state !== 'RUNNING' && !STARTING.has(p.state))
    .map((p) => d.api('POST', `/api/2.0/pipelines/${p.id}/updates`, {}).catch((e) => { throw new Error(`${p.label}: ${String(e)}`); })));
  const failed = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
  if (failed.length) actionError = failed.map((r) => String(r.reason).slice(0, 200)).join('; ');
  requestedAt = Date.now();
  refreshedAt = 0;
  await refresh();
}

/** Warm up: run prepare (wake the online store), then start every live pipeline that is not already running.
 *  Returns once the request is under way; prepare can take a minute, and the status reports it as starting. */
export async function warmUp(): Promise<PipelineStatus> {
  actionError = null;
  pauseWhenStopped = false;
  autoOffAt = null;
  refreshedAt = 0;
  await refresh();
  if (readError) throw new Error(readError);
  // A pipeline that is still stopping cannot start yet: queue the request and start once it is off.
  if (pipes.some((p) => p.state === 'STOPPING' || (p.update !== null && STOPPING_UPDATE.has(p.update)))) {
    queued = true;
    return pipelineStatus();
  }
  const d = deps!;
  if (!d.prepare) {
    await startStopped(d);
    return pipelineStatus();
  }
  cancelWarm = false;
  if (!preparing) {
    preparing = true;
    requestedAt = Date.now();
    void d.prepare()
      .then(() => (cancelWarm ? undefined : startStopped(d)))
      .catch((e) => { actionError = String(e).slice(0, 300); })
      .finally(() => { preparing = false; });
  }
  return pipelineStatus();
}

/** Turn off: stop every live pipeline that is running or starting, then run whenStopped once they are all down. */
export async function turnOff(): Promise<PipelineStatus> {
  actionError = null;
  refreshedAt = 0;
  await refresh();
  if (readError) throw new Error(readError);
  const d = deps!;
  await Promise.all(pipes.filter((p) => p.state !== 'IDLE' && p.state !== 'STOPPING')
    .map((p) => d.api('POST', `/api/2.0/pipelines/${p.id}/stop`)));
  requestedAt = 0;
  queued = false;
  cancelWarm = preparing; // a Warm up still preparing does not start the pipelines afterwards
  pauseWhenStopped = !!d.whenStopped;
  refreshedAt = 0;
  await refresh();
  await afterStop(d);
  return pipelineStatus();
}

async function afterStop(d: PipelineDeps) {
  if (!pauseWhenStopped || !loaded || preparing || queued || pipes.some((p) => ACTIVE.has(p.state))) return;
  pauseWhenStopped = false;
  await d.whenStopped?.().catch((e) => { actionError = String(e).slice(0, 300); });
}

/** One watchdog pass: finish a pending Turn off, and stop ingestion that runs with no Real-Time Mode pipeline. */
export async function watch(): Promise<void> {
  const d = deps;
  if (!d) return;
  refreshedAt = 0;
  await refresh();
  if (readError || !loaded) return;
  await afterStop(d);
  if (await autoOff(d)) return;
  const rtmActive = pipes.some((p) => p.role === 'rtm' && ACTIVE.has(p.state));
  if (rtmActive || preparing || queued || Date.now() - requestedAt < WARM_GRACE_MS) return;
  const idleIngest = pipes.filter((p) => p.role === 'ingestion' && (p.state === 'RUNNING' || STARTING.has(p.state)));
  for (const p of idleIngest) {
    console.log(`[watchdog] stopping ${p.label} (${p.id}): no Real-Time Mode pipeline is running`);
    await d.api('POST', `/api/2.0/pipelines/${p.id}/stop`).catch((e) => console.warn('[watchdog] stop failed:', String(e).slice(0, 200)));
  }
  if (idleIngest.length) {
    refreshedAt = 0;
    await refresh();
  }
}

/** Auto-off countdown for the UI banner, from the last pipeline read (refreshed every few seconds). */
export function autoOffStatus(): PipelineStatus['auto_off'] {
  const a = deps?.autoOff;
  if (!a) return null;
  if (Date.now() - refreshedAt > REFRESH_MS) void refresh();
  const running = pipes.some((p) => p.state === 'RUNNING' || STARTING.has(p.state));
  const waiting = !running || preparing || queued || pauseWhenStopped || a.busy();
  const left = a.idleMs - (Date.now() - Math.max(a.lastActivity(), requestedAt));
  return {
    idle_min: Math.round(a.idleMs / 60_000),
    turned_off_at: autoOffAt,
    off_in_s: waiting ? null : Math.max(0, Math.ceil(left / 1000)),
  };
}

/** Turn the demo off when it has run idleMs with no user activity (Warm up counts as activity). */
async function autoOff(d: PipelineDeps): Promise<boolean> {
  const a = d.autoOff;
  if (!a || preparing || queued || pauseWhenStopped || a.busy()) return false;
  const running = pipes.filter((p) => p.state === 'RUNNING' || STARTING.has(p.state));
  if (!running.length) return false;
  const idle = Date.now() - Math.max(a.lastActivity(), requestedAt);
  if (idle < a.idleMs) return false;
  console.log(`[auto-off] no user activity for ${Math.round(idle / 60_000)} min: turning off ${running.length} pipeline(s)`);
  a.before?.();
  await turnOff();
  autoOffAt = Date.now();
  return true;
}

export function startPipelines(d: PipelineDeps, watchMs = WATCH_MS) {
  deps = d;
  if (watchTimer) clearInterval(watchTimer);
  watchTimer = watchMs > 0 ? setInterval(() => void watch(), watchMs) : null;
  watchTimer?.unref();
}
