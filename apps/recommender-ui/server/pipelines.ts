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
}

const REFRESH_MS = 4_000;
const RESOLVE_MS = 5 * 60_000;
// Ready needs this many test events in a row servable within PROBE_OK_MS, not just updates in RUNNING.
const PROBE_OK_MS = 1_000;
const PROBES_NEEDED = 3;
const STARTING = new Set(['STARTING', 'DEPLOYING', 'RECOVERING', 'RESETTING']);
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
  let phase: Phase = !loaded ? (readError ? 'unknown' : 'checking') : phaseOf(pipes, now - requestedAt < 30_000);
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
  };
}

/** Warm up: start every live pipeline that is not already running. */
export async function warmUp(): Promise<PipelineStatus> {
  actionError = null;
  refreshedAt = 0;
  await refresh();
  if (readError) throw new Error(readError);
  // A pipeline that is still stopping cannot start yet: queue the request and start once it is off.
  if (pipes.some((p) => p.state === 'STOPPING' || (p.update !== null && STOPPING_UPDATE.has(p.update)))) {
    queued = true;
    return pipelineStatus();
  }
  const d = deps!;
  const results = await Promise.allSettled(pipes.filter((p) => p.state !== 'RUNNING' && !STARTING.has(p.state))
    .map((p) => d.api('POST', `/api/2.0/pipelines/${p.id}/updates`, {}).catch((e) => { throw new Error(`${p.label}: ${String(e)}`); })));
  const failed = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
  if (failed.length) actionError = failed.map((r) => String(r.reason).slice(0, 200)).join('; ');
  requestedAt = Date.now();
  refreshedAt = 0;
  await refresh();
  return pipelineStatus();
}

/** Turn off: stop every live pipeline that is running or starting. */
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
  refreshedAt = 0;
  await refresh();
  return pipelineStatus();
}

export function startPipelines(d: PipelineDeps) {
  deps = d;
}
