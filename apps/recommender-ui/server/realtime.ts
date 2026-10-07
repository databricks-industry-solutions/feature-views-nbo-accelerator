// ---------------------------------------------------------------------------
// Live numbers for the Lakeshore Bank site and the scale dashboard. Everything here is measured:
//   - request latency for every ranking and profile call this app made,
//   - stream health from the data itself: events/sec landing in the Kafka ingestion table, and
//     event -> online-store freshness (commit_time - event_time) on the streaming online table,
//   - the "Simulate live traffic" job (notebook 08, continuous producer) started and stopped through the
//     Jobs API. The app never talks to Kafka: Databricks Apps can't open outbound connections to the
//     MSK broker port, and the site's own clicks are request-time features, so it doesn't need to.
// ---------------------------------------------------------------------------

export interface LiveCfg {
  host: string;
  fq: string; // `catalog`.schema
  catalog: string;
  schema: string;
  rankerEndpoint: string;
  trafficJobId: string;
  getToken: () => Promise<string>; // app identity: Jobs API + Feature Engineering metadata
  query: (sql: string) => Promise<Record<string, unknown>[]>; // SQL warehouse
}

// ── Request metrics (ring buffers of what this app served) ───────────────────
const WINDOW_MS = 5 * 60_000;
type Sample = { t: number; v: number };
const recommendationEvents: { t: number }[] = [];
const latencyTest: Sample[] = [];
const profLat: Sample[] = [];
const top1: { t: number; cat: string }[] = [];
const streamEps: Sample[] = [];
const streamFresh: Sample[] = [];
let trafficActive = true;

function prune<T extends { t: number }>(a: T[]): T[] {
  const cut = Date.now() - WINDOW_MS;
  while (a.length && a[0].t < cut) a.shift();
  return a;
}
const pct = (xs: number[], p: number) => {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};

// ── Stream stats from the data (cached; one small query per 10 s at most) ──
interface StreamStats {
  at: number;
  ingest_count_60s: number;
  ingest_eps: number | null;
  fresh_n: number;
  fresh_p50_s: number | null;
  fresh_p95_s: number | null;
  error?: string;
}
let stats: StreamStats | null = null;
let statsInflight: Promise<StreamStats> | null = null;
let onlineTable: { name: string; at: number } | null = null;

interface ProducerHeartbeat {
  running: boolean;
  enabled: boolean;
  started_at: number;
  sent: number;
  delivered: number;
  target_eps: number;
  updated_at: number;
  error?: string;
}
let heartbeat: ProducerHeartbeat | null = null;
let heartbeatRate = 0;
let heartbeatReadAt = 0;
let heartbeatInflight: Promise<ProducerHeartbeat | null> | null = null;

async function loadProducerHeartbeat(cfg: LiveCfg): Promise<ProducerHeartbeat | null> {
  if (Date.now() - heartbeatReadAt < 150) return heartbeat;
  if (heartbeatInflight) return heartbeatInflight;
  heartbeatInflight = (async () => {
    try {
      const token = await cfg.getToken();
      const path = `/Volumes/${cfg.catalog}/${cfg.schema}/nbo_control/traffic_status.json`;
      const r = await fetch(`${cfg.host}/api/2.0/fs/files${path}`, { headers: { Authorization: `Bearer ${token}` } });
      if (!r.ok) throw new Error(`producer heartbeat ${r.status}: ${(await r.text()).slice(0, 200)}`);
      const next = JSON.parse(await r.text()) as ProducerHeartbeat;
      if (heartbeat && next.updated_at > heartbeat.updated_at) {
        const elapsed = (next.updated_at - heartbeat.updated_at) / 1000;
        heartbeatRate = elapsed > 0 ? Math.max(0, next.delivered - heartbeat.delivered) / elapsed : heartbeatRate;
        streamEps.push({ t: next.updated_at, v: heartbeatRate });
      }
      heartbeat = next;
      heartbeatReadAt = Date.now();
      return heartbeat;
    } catch {
      heartbeatReadAt = Date.now();
      return heartbeat;
    } finally {
      heartbeatInflight = null;
    }
  })();
  return heartbeatInflight;
}

async function streamingOnlineTable(cfg: LiveCfg): Promise<string | null> {
  if (onlineTable && Date.now() - onlineTable.at < 10 * 60_000) return onlineTable.name;
  const token = await cfg.getToken();
  const f = encodeURIComponent(`${cfg.catalog}.${cfg.schema}.cust_clicks_10m`);
  const r = await fetch(`${cfg.host}/api/2.0/feature-engineering/materialized-features?feature_name=${f}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) throw new Error(`materialized-features ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const j = (await r.json()) as { materialized_features?: { table_name?: string; online_store_config?: unknown }[] };
  const name = (j.materialized_features ?? []).find((m) => m.online_store_config && m.table_name)?.table_name ?? null;
  if (name) onlineTable = { name, at: Date.now() };
  return name;
}

async function loadStreamStats(cfg: LiveCfg): Promise<StreamStats> {
  if (stats && Date.now() - stats.at < 800) return stats;
  if (statsInflight) return statsInflight;
  statsInflight = (async () => {
    try {
      const tbl = await streamingOnlineTable(cfg);
      if (!tbl) throw new Error('streaming online table for cust_clicks_10m not found');
      // Freshness is commit_time - event_time per event on the online table (the true event -> servable
      // latency). Window-expiry rows (event_time NULL) and timer rows carry no source event, so they are excluded.
      const [row] = await cfg.query(`
        WITH f AS (
          SELECT DISTINCT customer_id, event_time,
                 unix_millis(to_timestamp(commit_time)) - unix_millis(to_timestamp(event_time)) AS lag_ms
          FROM ${tbl}
          WHERE event_time IS NOT NULL AND is_timer = false
            AND commit_time > current_timestamp() - INTERVAL 60 SECONDS
            AND event_time > current_timestamp() - INTERVAL 60 SECONDS
        )
        SELECT (SELECT count(*) FROM ${cfg.fq}.session_events_ingest
                WHERE stream_record_timestamp > current_timestamp() - INTERVAL 60 SECONDS) AS ingest_60s,
               count(*) AS n, percentile_approx(lag_ms, 0.5) AS p50, percentile_approx(lag_ms, 0.95) AS p95
        FROM f`);
      const num = (v: unknown) => (v === null || v === undefined || v === '' ? null : Number(v));
      const n = Number(row?.n ?? 0);
      const s: StreamStats = {
        at: Date.now(),
        ingest_count_60s: Number(row?.ingest_60s ?? 0),
        ingest_eps: num(row?.ingest_60s) === null ? null : Number(row?.ingest_60s) / 60,
        fresh_n: n,
        fresh_p50_s: n ? (num(row?.p50) ?? 0) / 1000 : null,
        fresh_p95_s: n ? (num(row?.p95) ?? 0) / 1000 : null,
      };
      if (s.fresh_p50_s !== null) streamFresh.push({ t: s.at, v: s.fresh_p50_s });
      stats = s;
      return s;
    } catch (e) {
      stats = {
        at: Date.now(), ingest_count_60s: 0, ingest_eps: null, fresh_n: 0,
        fresh_p50_s: null, fresh_p95_s: null, error: String(e).slice(0, 300),
      };
      return stats;
    } finally {
      statsInflight = null;
    }
  })();
  return statsInflight;
}

export const metrics = {
  recommendation(topCategory?: string) {
    const t = Date.now();
    recommendationEvents.push({ t });
    if (topCategory) top1.push({ t: Date.now(), cat: topCategory });
  },
  latencySample(ms: number, reset = false) {
    if (reset) latencyTest.length = 0;
    latencyTest.push({ t: Date.now(), v: ms });
    const values = latencyTest.map((s) => s.v);
    return {
      latency_ms: ms,
      p50_ms: pct(values, 50),
      p95_ms: pct(values, 95),
      p99_ms: pct(values, 99),
      samples: values.length,
    };
  },
  resetLatencyMeasurement() { latencyTest.length = 0; },
  setTrafficActive(active: boolean) {
    trafficActive = active;
  },
  profile(ms: number) { profLat.push({ t: Date.now(), v: ms }); },
  async snapshot(cfg: LiveCfg) {
    // The warehouse aggregation takes 1–3 seconds. Block only for the first sample; after that,
    // refresh it in the background so the dashboard's one-second poll always returns immediately.
    let ss = stats;
    if (!ss) ss = await loadStreamStats(cfg);
    else void loadStreamStats(cfg);
    let hb = heartbeat;
    if (!hb) hb = await loadProducerHeartbeat(cfg);
    else void loadProducerHeartbeat(cfg);
    [recommendationEvents, latencyTest, profLat, top1, streamEps, streamFresh].forEach((a) => prune(a as { t: number }[]));
    const now = Date.now();
    const span = Math.max(1, Math.min(WINDOW_MS, now - (recommendationEvents[0]?.t ?? now)) / 1000);
    // One-second buckets so the explicit latency test visibly updates in real time.
    const bucket = (a: Sample[]) => {
      const m = new Map<number, number[]>();
      for (const s of a) {
        const k = Math.floor(s.t / 1_000) * 1_000;
        if (!m.has(k)) m.set(k, []);
        m.get(k)!.push(s.v);
      }
      return [...m.entries()].sort((x, y) => x[0] - y[0]);
    };
    const mix: Record<string, number> = {};
    top1.forEach((x) => (mix[x.cat] = (mix[x.cat] ?? 0) + 1));
    Object.keys(mix).forEach((k) => (mix[k] = mix[k] / top1.length));
    return {
      window_s: WINDOW_MS / 1000,
      recommend: {
        count: recommendationEvents.length,
        qps: recommendationEvents.length / span,
        latest_ms: latencyTest.length ? latencyTest[latencyTest.length - 1].v : null,
        p50_ms: pct(latencyTest.map((s) => s.v), 50),
        p95_ms: pct(latencyTest.map((s) => s.v), 95),
        p99_ms: pct(latencyTest.map((s) => s.v), 99),
        latency_samples: latencyTest.length,
        measured_at: latencyTest.length ? latencyTest[latencyTest.length - 1].t : null,
        series: bucket(latencyTest).map(([t, v]) => ({
          t, p50_ms: pct(v, 50), p95_ms: pct(v, 95), p99_ms: pct(v, 99), count: v.length,
        })),
      },
      // Kafka -> Lakehouse ingestion rate, from the ingestion table (last 60 s).
      events: {
        count: ss.ingest_count_60s,
        eps: trafficActive && hb?.enabled ? heartbeatRate : 0,
        total: hb?.delivered ?? 0,
        measured_at: hb?.updated_at ?? ss.at,
        series: streamEps.map((s) => ({ t: s.t, count: Math.round(s.v) })),
      },
      // Event -> online-store freshness on the streaming online table (last 60 s).
      freshness: {
        samples: ss.fresh_n,
        p50_s: ss.fresh_p50_s,
        p95_s: ss.fresh_p95_s,
        measured_at: ss.at,
        series: streamFresh.map((s) => ({ t: s.t, s: s.v })),
      },
      stream_error: ss.error ?? null,
      // Feature Serving round-trip is retained for diagnostics, but is not the server-side lookup SLO.
      profile_round_trip: { p50_ms: pct(profLat.map((s) => s.v), 50) },
      top1_mix: mix,
      features: [
        { name: 'ctx_session_cat_views', window: 'This visit', source: 'Request', mode: 'request-time', freshness_s: null },
        { name: 'cust_mobile_cat_views_10m', window: 'Rolling 10m', source: 'Kafka', mode: 'streaming', freshness_s: ss.fresh_p50_s },
        { name: 'cust_clicks_10m', window: 'Rolling 10m', source: 'Kafka', mode: 'streaming', freshness_s: ss.fresh_p50_s },
        { name: 'cust_cat_views_30d', window: 'Sawtooth 30d', source: 'Kafka', mode: 'streaming', freshness_s: null },
        { name: 'cust_spend_to_income', window: 'Custom UDF', source: 'Feature views', mode: 'on demand', freshness_s: null },
        { name: 'cust_avg_balance_30d', window: 'Sliding 30d / 1d', source: 'Delta', mode: 'batch', freshness_s: null },
        { name: 'cust_spend_90d', window: 'Tumbling 90d', source: 'Delta', mode: 'batch', freshness_s: null },
        { name: 'cust_loyalty_tier', window: 'Latest value', source: 'Delta', mode: 'batch', freshness_s: null },
      ],
    };
  },
};

// ── "Simulate live traffic": the notebook 08 continuous producer, as a job ──
export interface TrafficState { running: boolean; run_id: number | null; state: string; started_at: number | null; url: string | null }
let trafficDesired = true;

async function jobsApi(cfg: LiveCfg, path: string, body?: unknown): Promise<Record<string, unknown>> {
  const token = await cfg.getToken();
  const r = await fetch(`${cfg.host}/api/2.2/jobs/${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Authorization: `Bearer ${token}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`jobs/${path.split('?')[0]} ${r.status}: ${(await r.text()).slice(0, 300)}`);
  return (await r.json()) as Record<string, unknown>;
}

async function writeTrafficControl(cfg: LiveCfg, enabled: boolean): Promise<void> {
  const token = await cfg.getToken();
  const path = `/Volumes/${cfg.catalog}/${cfg.schema}/nbo_control/traffic.json`;
  const r = await fetch(`${cfg.host}/api/2.0/fs/files${path}?overwrite=true`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/octet-stream' },
    body: JSON.stringify({ enabled, updated_at: new Date().toISOString() }),
  });
  if (!r.ok) throw new Error(`traffic control ${r.status}: ${(await r.text()).slice(0, 300)}`);
}

async function rawTrafficStatus(cfg: LiveCfg): Promise<TrafficState> {
  if (!cfg.trafficJobId) throw new Error('TRAFFIC_JOB_ID is not set (bind the traffic simulator job to the app).');
  const j = await jobsApi(cfg, `runs/list?job_id=${cfg.trafficJobId}&active_only=true&limit=1`);
  const run = ((j.runs as Record<string, unknown>[] | undefined) ?? [])[0];
  if (!run) return { running: false, run_id: null, state: 'IDLE', started_at: null, url: null };
  const st = (run.state as { life_cycle_state?: string } | undefined)?.life_cycle_state ?? 'UNKNOWN';
  return { running: true, run_id: Number(run.run_id), state: st, started_at: Number(run.start_time) || null, url: String(run.run_page_url ?? '') || null };
}

export async function trafficStatus(cfg: LiveCfg): Promise<TrafficState> {
  const state = await rawTrafficStatus(cfg);
  // A cancel is asynchronous. Once Stop is clicked, report the user's desired state immediately
  // instead of briefly flipping the button back to "Stop traffic" while the run winds down.
  return trafficDesired ? state : { ...state, running: false, state: state.running ? 'PAUSED' : 'IDLE' };
}

async function startTrafficRun(cfg: LiveCfg): Promise<TrafficState> {
  const cur = await rawTrafficStatus(cfg);
  if (cur.running) return cur;
  await jobsApi(cfg, 'run-now', { job_id: Number(cfg.trafficJobId) });
  return trafficStatus(cfg);
}

export async function trafficStart(cfg: LiveCfg): Promise<TrafficState> {
  trafficDesired = true;
  metrics.setTrafficActive(true);
  await writeTrafficControl(cfg, true);
  return startTrafficRun(cfg);
}

// App startup/keepalive may ensure traffic is running, but must respect an explicit Stop click.
export async function trafficEnsure(cfg: LiveCfg): Promise<TrafficState> {
  if (!trafficDesired) return trafficStatus(cfg);
  await writeTrafficControl(cfg, true);
  const state = await startTrafficRun(cfg);
  metrics.setTrafficActive(true);
  return state;
}

export async function trafficStop(cfg: LiveCfg): Promise<TrafficState> {
  trafficDesired = false;
  metrics.setTrafficActive(false);
  const cur = await rawTrafficStatus(cfg);
  await writeTrafficControl(cfg, false);
  // Keep the warm job and Kafka connection alive; only event generation pauses.
  return { ...cur, running: false, state: 'PAUSED' };
}
