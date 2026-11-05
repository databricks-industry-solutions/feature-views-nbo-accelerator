// ---------------------------------------------------------------------------
// Live traffic, the same setup as the card fraud console: the app itself writes click events to Kafka
// (Amazon MSK, IAM auth through the UC service credential), then polls the route-optimized Feature
// Serving endpoint until each event shows up in the served streaming features. Send time and read time
// come from the same clock, so event -> servable needs no cross-host clock assumptions.
// ---------------------------------------------------------------------------
import { randomUUID } from 'node:crypto';
import { Kafka, type Producer, logLevel, generateAuthTokenFromCredentialsProvider } from './vendor/kafka.js';

export interface StreamDeps {
  host: string;
  getToken: () => Promise<string>; // app service principal
  connection: string; // UC Kafka connection holding bootstrap_servers
  credential: string; // UC service credential for MSK IAM
  topic: string;
  // callsMs: one entry per Feature Serving request (one row each on the Live stream page).
  lookup: (rows: Record<string, unknown>[]) => Promise<{ outputs: Record<string, unknown>[]; latencyMs: number; callsMs?: number[]; readAt?: number }>;
  customers: () => Promise<string[]>;
  // Data freshness (README metric, lag = commit_time - event_time): each timed key's row in the streaming online
  // tables. Absent or null when Lakebase is not readable; the tiles then show only the Feature Serving timing.
  freshness?: (clickKeys: string[], mobileKeys: { customer_id: string; category: string }[]) =>
    Promise<{ clicks: Map<string, { event_ms: number; commit_ms: number }>; mobile: Map<string, { event_ms: number; commit_ms: number }> } | null>;
  produce?: (events: ClickEvent[]) => Promise<void>; // tests inject a fake Kafka
  warmupMs?: number;
  paused?: () => boolean; // demo paused (power.ts): no keep-warm reads or warm tests, so the endpoint can scale to zero
}

// Streaming features one click updates. cust_mobile_cat_views_10m counts only non-web clicks (nb07).
export const STREAMING = {
  cust_clicks_10m: { key: (e: ClickEvent) => e.customer_id },
  cust_mobile_cat_views_10m: { key: (e: ClickEvent) => `${e.customer_id}|${e.product_category}`, mobileOnly: true },
} as const;
type FeatureName = keyof typeof STREAMING;

export interface ClickEvent {
  event_id: string;
  customer_id: string;
  event_time: string;
  event_type: string;
  product_category: string;
  dwell_ms: number;
  device: string;
}
interface Target {
  feature: FeatureName;
  key: string;
  expect: number;
  value?: number;
  last_miss?: number;
  served_at?: number;
  resolution_ms?: number;
  fresh_ms?: number; // data freshness: the online table's commit_time - this event's event_time
}
export interface Tracked {
  seq: number;
  event: ClickEvent;
  source: string;
  sent_at: number;
  event_ms: number;
  targets: Target[];
  status: 'pending' | 'served' | 'missing';
}

const SERVING_WINDOW_MS = 10_000;
const RECENT_RETENTION_MS = 70_000;
const MISSING_AFTER_MS = 30_000;
// After Start, this long of untracked traffic warms the RTM pipeline before measuring begins. A cold run
// otherwise shows a 1-3 s Kafka -> RTM catch-up in its first seconds (seen at 00:44:08 UTC, 2026-10-17).
const WARMUP_MS = 10_000;
// Above this rate, each extra event is plain load: sent to Kafka on a repeat-visitor key, not timed.
// Timing every event at 1,000/s would mean ~100k chart points and a Feature Serving read for each.
const MEASURED_EPS = 100;
const LOAD_KEYS = 2_000;
const KAFKA_WARM_TIMEOUT_MS = 15_000;
const KAFKA_DIAG = /retr|leader|throttl|timed? ?out|disconnect|reconnect|connecting|refresh|ECONN|ETIMEDOUT|authenticat/i;
// Keep-warm: while someone has the app open, a trickle of untracked events keeps the Kafka connections and the
// RTM pipelines hot, so Start measures from its first second.
const KEEP_WARM_IDLE_MS = 3 * 60_000; // stop this long after the last browser request
const KEEP_WARM_READ_EVERY_MS = 2_000;
const BASELINE_WARMUPS = 12;
const BASELINE_CALLS = 40;
const BASELINE_EVERY_MS = 10 * 60_000;
// Endpoint warm calls stop this long after the last browser request, so idle endpoints scale to zero.
export const VISIT_WINDOW_MS = 30 * 60_000;
const CATEGORIES = ['credit_card', 'savings', 'personal_loan', 'mortgage', 'investment'];
const EVENT_TYPES = ['page_view', 'product_view', 'calculator_use', 'add_to_cart', 'search'];
const DEVICES = ['ios', 'android', 'web'];

let deps: StreamDeps | null = null;
const warmupMs = () => deps?.warmupMs ?? WARMUP_MS;

// ── Kafka (MSK IAM through the UC service credential) ────────────────────────
let kafka: { producer: Producer; connected: Promise<void> } | null = null;
let awsCreds: { accessKeyId: string; secretAccessKey: string; sessionToken: string; exp: number } | null = null;

async function dbx<T>(d: StreamDeps, method: string, path: string, body?: unknown): Promise<T> {
  const r = await fetch(`${d.host}${path}`, {
    method,
    headers: { Authorization: `Bearer ${await d.getToken()}`, 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`${path} ${r.status}: ${(await r.text()).slice(0, 300)}`);
  return (await r.json()) as T;
}

async function credentials(d: StreamDeps) {
  if (awsCreds && awsCreds.exp > Date.now() + 5 * 60_000) return awsCreds;
  const c = await dbx<{
    aws_temp_credentials: { access_key_id: string; secret_access_key: string; session_token: string };
    expiration_time?: number;
  }>(d, 'POST', '/api/2.1/unity-catalog/temporary-service-credentials', { credential_name: d.credential });
  const t = c.aws_temp_credentials;
  awsCreds = {
    accessKeyId: t.access_key_id, secretAccessKey: t.secret_access_key, sessionToken: t.session_token,
    exp: c.expiration_time ?? Date.now() + 50 * 60_000,
  };
  return awsCreds;
}

async function producer(d: StreamDeps): Promise<Producer> {
  if (!kafka) {
    const conn = await dbx<{ options?: { bootstrap_servers?: string } }>(d, 'GET', `/api/2.1/unity-catalog/connections/${d.connection}`);
    const brokers = (conn.options?.bootstrap_servers ?? '').split(',').map((b) => b.trim()).filter(Boolean);
    if (!brokers.length) throw new Error(`UC connection ${d.connection} has no bootstrap_servers`);
    const region = /kafka\.([a-z0-9-]+)\.amazonaws\.com/.exec(brokers[0])?.[1] ?? 'us-west-2';
    const client = new Kafka({
      clientId: 'nbo-recommender-ui',
      brokers,
      ssl: true,
      // DEBUG so retries and reconnects are visible, but only those lines (and WARN+) are printed: a send that
      // stalls ~2 s mid-run needs to show whether a broker retried, reconnected, or throttled.
      logLevel: logLevel.DEBUG,
      logCreator: () => ({ level, label, log }) => {
        if (level > logLevel.WARN && !KAFKA_DIAG.test(log.message ?? '')) return;
        const { timestamp: _t, message, ...extra } = log;
        console.warn(`[kafkajs] ${label} ${message} ${JSON.stringify(extra).slice(0, 300)}`);
      },
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          const { token } = await generateAuthTokenFromCredentialsProvider({ region, awsCredentialsProvider: () => credentials(d) });
          return { value: token };
        },
      },
    });
    const p = client.producer({ allowAutoTopicCreation: false });
    // Per-broker timing of slow requests: pendingDuration is time queued in the client, duration the round trip.
    p.on(p.events.REQUEST, ({ payload: r }) => {
      if (r.duration + r.pendingDuration > 500) {
        console.warn(`[kafka] ${r.apiName} to ${r.broker} took ${r.duration} ms (+${r.pendingDuration} ms queued, ${r.size} bytes)`);
      }
    });
    p.on(p.events.REQUEST_TIMEOUT, ({ payload: r }) => console.warn(`[kafka] ${r.apiName} to ${r.broker} timed out`));
    kafka = { producer: p, connected: p.connect() };
    kafka.connected.catch(() => { kafka = null; });
  }
  await kafka.connected;
  return kafka.producer;
}

// Time of every Kafka send (call -> ack from all in-sync replicas), to tell a slow send from a slow pipeline.
const produceSamples: { at: number; ms: number }[] = [];

async function produceKafka(d: StreamDeps, events: ClickEvent[]) {
  const p = await producer(d);
  const t0 = Date.now();
  await p.send({
    topic: d.topic,
    acks: -1,
    messages: events.map((e) => ({ key: e.customer_id, value: JSON.stringify(e) })),
  });
  const ms = Date.now() - t0;
  produceSamples.push({ at: Date.now(), ms });
  if (produceSamples.length > 2_000) produceSamples.splice(0, produceSamples.length - 2_000);
  if (ms > 500) console.warn(`[kafka] send of ${events.length} events took ${ms} ms`);
}

// ── tracker ──────────────────────────────────────────────────────────────────
export const nowIso = () => new Date().toISOString();
let seq = 0;
const count = new Map<string, number>();
const pending = new Map<string, Tracked>();
const recent: Tracked[] = [];
// Every served target and every finished event, in the order they resolved. The browser reads these with a
// cursor, so a slow or late poll never drops a point from the chart or a row from the table.
const servedLog: { at: number; id: string; t: number; v: number }[] = [];
// Data freshness per timed event (click count), for the chart: same cursor as servedLog.
const freshLog: { at: number; id: string; t: number; v: number }[] = [];
const finished: { at: number; tracked: Tracked }[] = [];
let lastAt = 0;
const stamp = () => (lastAt = Math.max(Date.now(), lastAt + 0.001)); // strictly increasing cursor values
const servingSamples: { at: number; ms: number }[] = [];
let pollError = '';
let baseline: { p50: number | null; p95: number | null; p99: number | null; n: number; at: number | null; error: string | null } =
  { p50: null, p95: null, p99: null, n: 0, at: null, error: null };

function matches(feature: FeatureName, event: ClickEvent): boolean {
  const spec = STREAMING[feature];
  return !('mobileOnly' in spec) || event.device !== 'web';
}

// Untracked load events written since measuring began (counted in events/s, never timed).
const loadSamples: { at: number; n: number }[] = [];

/** Write events to Kafka without live-traffic tracking (the Lakeshore Bank tab's real visitor clicks). */
export async function produceEvents(events: ClickEvent[]): Promise<void> {
  const d = deps;
  if (!d) throw new Error('stream module not started');
  await (d.produce ? d.produce(events) : produceKafka(d, events));
}

export async function send(events: ClickEvent[], source: string, load: ClickEvent[] = []): Promise<Tracked[]> {
  const d = deps;
  if (!d) throw new Error('stream module not started');

  // Track the events BEFORE handing them to Kafka, so Feature Serving reads start while they are in flight.
  // Those early reads come back "not yet", which brackets the moment each event became servable. Tracking
  // only after the Kafka ack meant the first read usually already hit, and the midpoint undercounted.
  const sentAt = Date.now();
  const trackedEvents = events.map((event) => {
    const targets = (Object.keys(STREAMING) as FeatureName[])
      .filter((feature) => matches(feature, event))
      .map((feature) => {
        const key = STREAMING[feature].key(event);
        const id = `${feature}|${key}`;
        const expect = (count.get(id) ?? 0) + 1;
        count.set(id, expect);
        return { feature, key, expect };
      });
    const tracked: Tracked = {
      seq: ++seq, event, source, sent_at: sentAt, event_ms: Date.parse(event.event_time), targets,
      status: targets.length ? 'pending' : 'served',
    };
    if (targets.length) pending.set(event.event_id, tracked);
    recent.push(tracked);
    return tracked;
  });
  try {
    const all = load.length ? [...events, ...load] : events;
    await (d.produce ? d.produce(all) : produceKafka(d, all));
    if (load.length) {
      loadSamples.push({ at: Date.now(), n: load.length });
      if (loadSamples.length > 400) loadSamples.splice(0, loadSamples.length - 400);
    }
  } catch (error) {
    // Not written to Kafka: stop tracking these events so they are not reported as missing.
    const failed = new Set(trackedEvents);
    for (const t of trackedEvents) pending.delete(t.event.event_id);
    for (let i = recent.length - 1; i >= 0; i -= 1) if (failed.has(recent[i])) recent.splice(i, 1);
    throw error;
  }
  const cutoff = sentAt - RECENT_RETENTION_MS;
  const firstCurrent = recent.findIndex((tracked) => tracked.sent_at >= cutoff);
  if (firstCurrent > 0) recent.splice(0, firstCurrent);
  else if (firstCurrent === -1) recent.length = 0;
  return trackedEvents;
}

async function poll(): Promise<void> {
  const d = deps;
  if (!d || !pending.size) return;
  // One row per (customer, category): that row returns both the customer- and the category-keyed feature.
  const rows = [...new Map([...pending.values()].reverse().map((t) => [`${t.event.customer_id}|${t.event.product_category}`, t.event])).values()].slice(0, 100);
  const started = Date.now();
  const { outputs, latencyMs, callsMs, readAt: dataPlaneReadAt } = await d.lookup(rows.map((e) => ({ customer_id: e.customer_id, product_category: e.product_category })));
  if (traffic && started >= traffic.started) {
    const at = Date.now();
    for (const ms of callsMs ?? [latencyMs]) servingSamples.push({ at, ms });
    if (servingSamples.length > 2_000) servingSamples.splice(0, servingSamples.length - 2_000);
  }

  const readAt = dataPlaneReadAt ?? (started + Date.now()) / 2;
  const latest = new Map<string, number>();
  outputs.forEach((output, index) => {
    const e = rows[index];
    if (!e) return;
    for (const feature of Object.keys(STREAMING) as FeatureName[]) {
      latest.set(`${feature}|${STREAMING[feature].key(e)}`, Number(output?.[feature] ?? 0));
    }
  });
  for (const [id, tracked] of pending) {
    for (const target of tracked.targets) {
      if (target.served_at !== undefined) continue;
      const value = latest.get(`${target.feature}|${target.key}`);
      if (value !== undefined && value >= target.expect) {
        // Servable = the first Feature Serving read that returned the update. Usually no read misses after the
        // event (the first read already hits), so a miss/hit midpoint would halve the real time; the first hit
        // is a conservative upper bound, at most one read (~25 ms) late. resolution_ms = width of that bracket.
        const miss = Math.max(target.last_miss ?? tracked.event_ms, tracked.event_ms);
        target.served_at = readAt;
        target.resolution_ms = readAt - miss;
        target.value = value;
        servedLog.push({ at: stamp(), id: `${tracked.event.event_id}|${target.feature}`, t: tracked.sent_at, v: target.served_at - tracked.event_ms });
      } else if (value !== undefined) {
        target.last_miss = readAt;
      }
    }
    if (tracked.targets.every((target) => target.served_at !== undefined)) {
      tracked.status = 'served';
      pending.delete(id);
      finished.push({ at: stamp(), tracked });
    } else if (Date.now() - tracked.sent_at > MISSING_AFTER_MS) {
      tracked.status = 'missing';
      pending.delete(id);
      finished.push({ at: stamp(), tracked });
    }
  }
  const cut = Date.now() - RECENT_RETENTION_MS;
  while (servedLog.length && servedLog[0].at < cut) servedLog.shift();
  while (freshLog.length && freshLog[0].at < cut) freshLog.shift();
  if (finished.length > 400) finished.splice(0, finished.length - 400);
}

export function pct(values: number[], percentile: number): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil((percentile / 100) * sorted.length) - 1))];
}

export function stats(windowMs = 60_000) {
  const now = Date.now();
  const since = now - windowMs;
  const warmFrom = traffic ? traffic.started : 0;
  const lag: number[] = [];
  const fresh: number[] = [];
  const resolution: number[] = [];
  let sent = 0, backgroundSent = 0, missing = 0;
  for (const tracked of recent) {
    if (tracked.sent_at < since) continue;
    sent++;
    if (tracked.source === 'traffic') backgroundSent++;
    if (tracked.status === 'missing') missing++;
    // The first seconds after Start hit a cold pipeline; they stay in the chart and table, not the percentiles.
    if (tracked.sent_at < warmFrom) continue;
    for (const target of tracked.targets) {
      // One freshness sample per event: its click-count row, the same table the benchmark notebook reads.
      if (target.feature === 'cust_clicks_10m' && target.fresh_ms !== undefined) fresh.push(target.fresh_ms);
      if (target.served_at === undefined) continue;
      lag.push(target.served_at - tracked.event_ms);
      resolution.push(target.resolution_ms ?? 0);
    }
  }
  const serving = servingSamples.filter((s) => s.at >= now - SERVING_WINDOW_MS).map((s) => s.ms);
  const observedMs = traffic ? Math.min(windowMs, Math.max(250, now - traffic.started)) : windowMs;
  const observedS = Math.max(0.25, observedMs / 1000);
  let loadSent = 0;
  for (let i = loadSamples.length - 1; i >= 0 && loadSamples[i].at >= since; i -= 1) loadSent += loadSamples[i].n;
  return {
    sent,
    missing,
    events_per_s: traffic ? (backgroundSent + loadSent) / observedS : 0,
    // Events per second timed end to end (all of them up to MEASURED_EPS, a fixed sample above it).
    measured_per_s: traffic ? backgroundSent / observedS : 0,
    pending: pending.size,
    poll_error: pollError || null,
    // Data freshness, lag = commit_time - event_time per timed event (README, "How data freshness is measured").
    freshness_p50: pct(fresh, 50),
    freshness_p95: pct(fresh, 95),
    freshness_p99: pct(fresh, 99),
    freshness_n: fresh.length,
    // Event -> the first Feature Serving read that returned it: freshness plus the app's read scheduling.
    served_p50: pct(lag, 50),
    served_p95: pct(lag, 95),
    served_p99: pct(lag, 99),
    n: lag.length,
    resolution_p50: pct(resolution, 50),
    feature_serving_p50: pct(serving, 50),
    feature_serving_p95: pct(serving, 95),
    feature_serving_p99: pct(serving, 99),
    feature_serving_n: serving.length,
    feature_serving_window_s: SERVING_WINDOW_MS / 1000,
    feature_serving_baseline_p50: baseline.p50,
    feature_serving_baseline_p95: baseline.p95,
    feature_serving_baseline_p99: baseline.p99,
    feature_serving_baseline_n: baseline.n,
    feature_serving_baseline_at: baseline.at,
    feature_serving_baseline_error: baseline.error,
    // Kafka send time per 250 ms batch, last 60 s.
    produce_p50: pct(produceSamples.filter((x) => x.at >= since).map((x) => x.ms), 50),
    produce_p95: pct(produceSamples.filter((x) => x.at >= since).map((x) => x.ms), 95),
    produce_max: pct(produceSamples.filter((x) => x.at >= since).map((x) => x.ms), 100),
  };
}

// Chart points resolved after the browser's cursor (all of them, never capped).
export function freshPointsSince(cursor: number) {
  let i = freshLog.length;
  while (i > 0 && freshLog[i - 1].at > cursor) i -= 1;
  return freshLog.slice(i).map(({ id, t, v }) => ({ id, t, v }));
}

export function pointsSince(cursor: number) {
  let i = servedLog.length;
  while (i > 0 && servedLog[i - 1].at > cursor) i -= 1;
  return servedLog.slice(i).map(({ id, t, v }) => ({ id, t, v }));
}

// The table: the newest finished events (served or missing), newest first.
export function latestFinished(n = 40): Tracked[] {
  return finished.slice(-n).reverse().map((f) => f.tracked);
}

export const cursorNow = () => lastAt;

export function feedSince(since: number): Tracked[] {
  return recent.filter((tracked) => tracked.sent_at >= since);
}

// Median event -> servable in seconds, for the feature table on How it works.
export function servableP50Seconds(): number | null {
  const v = stats().served_p50;
  return v === null ? null : v / 1000;
}

// ── traffic ──────────────────────────────────────────────────────────────────
const uid = () => randomUUID().replace(/-/g, '').slice(0, 12);
interface TrafficSession { eps: number; started: number; until: number; timer: NodeJS.Timeout | null; error: string }
let traffic: TrafficSession | null = null;

function clickEvent(customerId: string): ClickEvent {
  const id = randomUUID().replace(/-/g, '');
  return {
    event_id: `app_${id}`,
    customer_id: customerId,
    event_time: nowIso(),
    event_type: EVENT_TYPES[Math.floor(Math.random() * EVENT_TYPES.length)],
    product_category: CATEGORIES[Math.floor(Math.random() * CATEGORIES.length)],
    dwell_ms: 200 + Math.floor(Math.random() * 44_800),
    device: DEVICES[Math.floor(Math.random() * DEVICES.length)],
  };
}

// Open producer connections to every broker before the first timed event. After an idle period MSK has closed
// them, and the reconnect + IAM handshake would otherwise land on the first second of events.
async function warmKafka(d: StreamDeps) {
  const p = await producer(d);
  await p.send({
    topic: d.topic, acks: -1,
    messages: Array.from({ length: 24 }, (_, i) => {
      const e = clickEvent(`cust_warmup_${i}`);
      return { key: e.customer_id, value: JSON.stringify(e) };
    }),
  });
}

// A stalled MSK connect would otherwise hold the Start request open with no feedback in the UI.
function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not finish in ${ms / 1000} s, try Start again`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

// ── keep-warm ────────────────────────────────────────────────────────────────
let lastVisit = 0;
let warmSince = 0; // start of the current unbroken keep-warm stream, 0 while it is not flowing
let lastWarmSend = 0;
let lastWarmRead = 0;
let keepWarmBusy = false;
let keepWarmTimer: NodeJS.Timeout | null = null;

/** Called on every browser API request; keeps the pipeline warm while someone has the app open. */
export function touch(): void {
  lastVisit = Date.now();
}
export const lastVisitAt = () => lastVisit;

async function keepWarm(): Promise<void> {
  const d = deps;
  const now = Date.now();
  if (!d) return;
  if (traffic) {
    // Live traffic keeps everything hot by itself.
    if (!warmSince) warmSince = now;
    lastWarmSend = now;
    return;
  }
  if (now - lastVisit > KEEP_WARM_IDLE_MS || d.paused?.()) {
    warmSince = 0;
    return;
  }
  if (keepWarmBusy) return;
  keepWarmBusy = true;
  try {
    // Endpoint compute is kept warm by the server's 3-minute warm call; this read only keeps the live tracker's
    // Feature Serving connection open, so the first reads after Start are not slow.
    if (now - lastWarmRead >= KEEP_WARM_READ_EVERY_MS) {
      lastWarmRead = now;
      void d.lookup([{ customer_id: `cust_warmup_${uid()}`, product_category: 'savings' }]).catch(() => undefined);
    }
    const event = clickEvent(`cust_warmup_${uid()}`);
    await (d.produce ? d.produce([event]) : produceKafka(d, [event]));
    if (!warmSince) warmSince = Date.now();
    lastWarmSend = Date.now();
  } catch {
    warmSince = 0; // a failed send means the stream is not warm; Start falls back to the full warm-up
  } finally {
    keepWarmBusy = false;
  }
}

// How much of the Start warm-up is still needed: none once keep-warm has flowed for a full warm-up window.
function warmupLeftMs(): number {
  const now = Date.now();
  const flowing = warmSince > 0 && now - lastWarmSend < 2_000;
  return flowing ? Math.max(0, warmupMs() - (now - warmSince)) : warmupMs();
}

export async function startTraffic(eps: number, seconds: number): Promise<void> {
  stopTraffic();
  const warmLeft = warmupLeftMs();
  // A flowing keep-warm stream already holds open producer connections, so skip the connection warm-up too.
  if (deps && !deps.produce && warmLeft > 0) await withTimeout(warmKafka(deps), KAFKA_WARM_TIMEOUT_MS, 'Connecting to Kafka');
  pending.clear();
  recent.length = 0;
  servedLog.length = 0;
  freshLog.length = 0;
  finished.length = 0;
  servingSamples.length = 0;
  loadSamples.length = 0;
  count.clear();
  // `started` is when measuring begins; before it the same traffic flows untracked to warm the pipeline.
  const trackFrom = Date.now() + warmLeft;
  const session: TrafficSession = { eps, started: trackFrom, until: trackFrom + seconds * 1000, timer: null, error: '' };
  const tick = async () => {
    if (traffic !== session) return;
    if (Date.now() > session.until) {
      stopTraffic();
      return;
    }
    const warming = Date.now() < session.started;
    const total = Math.max(1, Math.round(session.eps / 4));
    try {
      if (warming) {
        const d = deps!;
        const events = Array.from({ length: total }, () => clickEvent(`cust_warmup_${uid()}`));
        await (d.produce ? d.produce(events) : produceKafka(d, events));
        return;
      }
      // Fresh visitor keys, so each timed event moves its features from 0 to 1 and can be timed on its own.
      const measured = Math.min(total, Math.round(MEASURED_EPS / 4));
      const events = Array.from({ length: measured }, () => clickEvent(`cust_live_${uid()}`));
      const load = Array.from({ length: total - measured }, () => clickEvent(`cust_load_${Math.floor(Math.random() * LOAD_KEYS)}`));
      await send(events, 'traffic', load);
      if (traffic === session) session.error = '';
    } catch (error) {
      if (traffic === session) session.error = String(error).slice(0, 300);
    }
  };
  session.timer = setInterval(tick, 250);
  traffic = session;
}

// The numbers of the last finished run, kept in server memory so a page refresh after a run still shows them.
export interface LastRun {
  started_at: number; ended_at: number; eps: number;
  stats: ReturnType<typeof stats>; feed: Tracked[]; points: { id: string; t: number; v: number }[];
  fresh_points: { id: string; t: number; v: number }[];
}
let lastRun: LastRun | null = null;
export const lastRunSnapshot = () => lastRun;

export function stopTraffic(): void {
  if (traffic?.timer) clearInterval(traffic.timer);
  if (traffic) {
    const s = stats();
    if (s.n > 0) {
      lastRun = {
        started_at: traffic.started, ended_at: Date.now(), eps: traffic.eps, stats: s,
        feed: latestFinished(40), points: servedLog.map(({ id, t, v }) => ({ id, t, v })),
        fresh_points: freshLog.map(({ id, t, v }) => ({ id, t, v })),
      };
    }
  }
  traffic = null;
}

export function trafficState() {
  return traffic
    ? {
        running: true,
        eps: traffic.eps,
        ends_in_s: Math.max(0, Math.round((traffic.until - Date.now()) / 1000)),
        warming_up: Date.now() < traffic.started,
        warm_left_s: Math.max(0, Math.ceil((traffic.started - Date.now()) / 1000)),
        error: traffic.error || null,
      }
    : { running: false };
}

// ── warm baseline ──────────────────────────────────────────────────────
async function runBaseline(d: StreamDeps) {
  try {
    const customers = await d.customers();
    if (!customers.length) throw new Error('no customers available for the warm test');
    const row = (i: number) => [{ customer_id: customers[i % customers.length], product_category: CATEGORIES[i % CATEGORIES.length] }];
    for (let i = 0; i < BASELINE_WARMUPS; i += 1) await d.lookup(row(i));
    const ms: number[] = [];
    for (let i = 0; i < BASELINE_CALLS; i += 1) ms.push((await d.lookup(row(BASELINE_WARMUPS + i))).latencyMs);
    baseline = { p50: pct(ms, 50), p95: pct(ms, 95), p99: pct(ms, 99), n: ms.length, at: Date.now(), error: null };
  } catch (e) {
    baseline = { ...baseline, error: String(e).slice(0, 300) };
  }
}

// Freshness probe for Warm up: one click on a fresh key, read back through Feature Serving until both tracked
// streaming features show it. Right after the pipelines report RUNNING, events still took ~21 s to become
// servable (06:10 UTC, 2026-10-22), so Warm up waits until this comes back fast before it says ready.
export async function probeFreshness(timeoutMs = 30_000): Promise<number> {
  const d = deps;
  if (!d) throw new Error('live stream not started');
  const event = { ...clickEvent(`cust_probe_${uid()}`), device: 'ios' }; // non-web, so both features count it
  const sentAt = Date.now();
  await (d.produce ? d.produce([event]) : produceKafka(d, [event]));
  while (Date.now() - sentAt < timeoutMs) {
    const { outputs } = await d.lookup([{ customer_id: event.customer_id, product_category: event.product_category }]);
    const o = outputs[0] ?? {};
    if (Number(o.cust_clicks_10m) >= 1 && Number(o.cust_mobile_cat_views_10m) >= 1) return Date.now() - sentAt;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`test event not servable after ${timeoutMs / 1000} s`);
}

// Read data freshness for timed events that do not have it yet, in batches. Timed events use fresh keys
// (cust_live_*), one event each, so each key's online row holds that event's first commit.
let freshBusy = false;
async function readFreshness(d: StreamDeps) {
  if (!d.freshness || freshBusy) return;
  const since = Date.now() - 30_000;
  const todo: { t: Tracked; target: Target }[] = [];
  for (let i = recent.length - 1; i >= 0 && todo.length < 400; i -= 1) {
    const t = recent[i];
    if (t.sent_at < since) break;
    if (!t.event.customer_id.startsWith('cust_live_')) continue;
    for (const target of t.targets) if (target.fresh_ms === undefined) todo.push({ t, target });
  }
  if (!todo.length) return;
  freshBusy = true;
  try {
    const clickKeys = [...new Set(todo.filter((x) => x.target.feature === 'cust_clicks_10m').map((x) => x.t.event.customer_id))];
    const mobileKeys = todo.filter((x) => x.target.feature === 'cust_mobile_cat_views_10m')
      .map((x) => ({ customer_id: x.t.event.customer_id, category: x.t.event.product_category }));
    const rows = await d.freshness(clickKeys, mobileKeys);
    if (!rows) return;
    for (const { t, target } of todo) {
      const r = target.feature === 'cust_clicks_10m' ? rows.clicks.get(t.event.customer_id) : rows.mobile.get(target.key);
      // Only this event's row counts (its own event_time); the commit_time - event_time lag is the freshness.
      if (!r || Math.abs(r.event_ms - t.event_ms) >= 1) continue;
      target.fresh_ms = Math.max(0, Math.round(r.commit_ms - t.event_ms));
      if (target.feature === 'cust_clicks_10m') freshLog.push({ at: stamp(), id: `${t.event.event_id}|fresh`, t: t.sent_at, v: target.fresh_ms });
    }
  } catch {
    // Freshness is best effort; the next pass retries.
  } finally {
    freshBusy = false;
  }
}

export function startStream(d: StreamDeps) {
  deps = d;
  setInterval(() => { if (traffic || recent.length) void readFreshness(d); }, 300);
  (function loop() {
    poll().then(() => { pollError = ''; }, (error) => { pollError = String(error).slice(0, 300); })
      .finally(() => setTimeout(loop, pending.size ? 0 : traffic ? 10 : 500));
  })();
  if (!keepWarmTimer) keepWarmTimer = setInterval(() => void keepWarm(), 250);
  // Warm Feature Serving test at startup; retried after a minute on failure, refreshed every 10 min while someone
  // has visited in the last 30 min. With no visitors it stops, so the endpoint can scale to zero.
  let first = true;
  (function baselineLoop() {
    const visited = first || Date.now() - lastVisit < VISIT_WINDOW_MS;
    first = false;
    const run = traffic || !visited || d.paused?.() ? Promise.resolve() : runBaseline(d);
    void run.finally(() => setTimeout(baselineLoop, baseline.error ? 60_000 : BASELINE_EVERY_MS));
  })();
  // Open the Kafka connection early so the first Start does not pay the TLS + IAM handshake.
  if (!d.produce) void producer(d).catch((e) => console.warn('[kafka] connect deferred:', String(e).slice(0, 200)));
}
