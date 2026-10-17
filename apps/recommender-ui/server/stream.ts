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
  lookup: (rows: Record<string, unknown>[]) => Promise<{ outputs: Record<string, unknown>[]; latencyMs: number; readAt?: number }>;
  rank: (customerId: string) => Promise<number>; // ranker data-plane latency in ms
  customers: () => Promise<string[]>;
  produce?: (events: ClickEvent[]) => Promise<void>; // tests inject a fake Kafka
  warmupMs?: number;
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
const RANK_WINDOW_MS = 30_000;
const RECENT_RETENTION_MS = 70_000;
const MISSING_AFTER_MS = 30_000;
// After Start, this long of untracked traffic warms the RTM pipeline before measuring begins. A cold run
// otherwise shows a 1-3 s Kafka -> RTM catch-up in its first seconds (seen at 00:44:08 UTC, 2026-10-17).
const WARMUP_MS = 10_000;
const RANK_EVERY_MS = 700; // same cadence as the fraud console's scoring sampler
const BASELINE_WARMUPS = 12;
const BASELINE_CALLS = 40;
const BASELINE_EVERY_MS = 10 * 60_000;
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
      logLevel: logLevel.WARN,
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          const { token } = await generateAuthTokenFromCredentialsProvider({ region, awsCredentialsProvider: () => credentials(d) });
          return { value: token };
        },
      },
    });
    const p = client.producer({ allowAutoTopicCreation: false });
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
const finished: { at: number; tracked: Tracked }[] = [];
let lastAt = 0;
const stamp = () => (lastAt = Math.max(Date.now(), lastAt + 0.001)); // strictly increasing cursor values
const servingSamples: { at: number; ms: number }[] = [];
const rankSamples: { at: number; ms: number }[] = [];
let pollError = '';
let rankError = '';
let baseline: { p50: number | null; p95: number | null; p99: number | null; n: number; at: number | null; error: string | null } =
  { p50: null, p95: null, p99: null, n: 0, at: null, error: null };

function matches(feature: FeatureName, event: ClickEvent): boolean {
  const spec = STREAMING[feature];
  return !('mobileOnly' in spec) || event.device !== 'web';
}

export async function send(events: ClickEvent[], source: string): Promise<Tracked[]> {
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
    await (d.produce ? d.produce(events) : produceKafka(d, events));
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
  const { outputs, latencyMs, readAt: dataPlaneReadAt } = await d.lookup(rows.map((e) => ({ customer_id: e.customer_id, product_category: e.product_category })));
  if (traffic && started >= traffic.started) {
    servingSamples.push({ at: Date.now(), ms: latencyMs });
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
      if (target.served_at === undefined) continue;
      lag.push(target.served_at - tracked.event_ms);
      resolution.push(target.resolution_ms ?? 0);
    }
  }
  const serving = servingSamples.filter((s) => s.at >= now - SERVING_WINDOW_MS).map((s) => s.ms);
  const ranks = rankSamples.filter((s) => s.at >= now - RANK_WINDOW_MS).map((s) => s.ms);
  const observedMs = traffic ? Math.min(windowMs, Math.max(250, now - traffic.started)) : windowMs;
  return {
    sent,
    missing,
    events_per_s: traffic ? backgroundSent / Math.max(0.25, observedMs / 1000) : 0,
    pending: pending.size,
    poll_error: pollError || null,
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
    // Ranker (online feature lookup + model) latency, sampled every 700 ms while traffic runs; used by How it works.
    serving_p50: pct(ranks, 50),
    serving_p95: pct(ranks, 95),
    serving_n: ranks.length,
    serving_error: rankError || null,
    // Kafka send time per 250 ms batch, last 60 s.
    produce_p50: pct(produceSamples.filter((x) => x.at >= since).map((x) => x.ms), 50),
    produce_p95: pct(produceSamples.filter((x) => x.at >= since).map((x) => x.ms), 95),
    produce_max: pct(produceSamples.filter((x) => x.at >= since).map((x) => x.ms), 100),
  };
}

// Chart points resolved after the browser's cursor (all of them, never capped).
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

export async function startTraffic(eps: number, seconds: number): Promise<void> {
  stopTraffic();
  if (deps && !deps.produce) await warmKafka(deps);
  pending.clear();
  recent.length = 0;
  servedLog.length = 0;
  finished.length = 0;
  servingSamples.length = 0;
  rankSamples.length = 0;
  count.clear();
  // `started` is when measuring begins; before it the same traffic flows untracked to warm the pipeline.
  const trackFrom = Date.now() + warmupMs();
  const session: TrafficSession = { eps, started: trackFrom, until: trackFrom + seconds * 1000, timer: null, error: '' };
  const tick = async () => {
    if (traffic !== session) return;
    if (Date.now() > session.until) {
      stopTraffic();
      return;
    }
    const warming = Date.now() < session.started;
    // Fresh visitor keys, so each event moves its features from 0 to 1 and can be timed on its own.
    const events = Array.from({ length: Math.max(1, Math.round(session.eps / 4)) }, () => clickEvent(warming ? `cust_warmup_${uid()}` : `cust_live_${uid()}`));
    try {
      if (warming) {
        const d = deps!;
        await (d.produce ? d.produce(events) : produceKafka(d, events));
        return;
      }
      await send(events, 'traffic');
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

// ── warm baseline + ranker load ──────────────────────────────────────────────
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

let rankCustomers: string[] = [];
let rankIdx = 0;
let rankInFlight = 0;
async function rankOne(d: StreamDeps) {
  if (rankInFlight >= 3) return;
  rankInFlight += 1;
  try {
    if (!rankCustomers.length) rankCustomers = await d.customers();
    if (!rankCustomers.length) throw new Error('no customers available to rank');
    rankSamples.push({ at: Date.now(), ms: await d.rank(rankCustomers[rankIdx++ % rankCustomers.length]) });
    if (rankSamples.length > 2_000) rankSamples.splice(0, rankSamples.length - 2_000);
    rankError = '';
  } catch (e) {
    rankError = String(e).slice(0, 300);
  } finally {
    rankInFlight -= 1;
  }
}

export function startStream(d: StreamDeps) {
  deps = d;
  (function loop() {
    poll().then(() => { pollError = ''; }, (error) => { pollError = String(error).slice(0, 300); })
      .finally(() => setTimeout(loop, pending.size ? 0 : traffic ? 10 : 500));
  })();
  setInterval(() => { if (traffic) void rankOne(d); }, RANK_EVERY_MS);
  // Warm Feature Serving test at startup; retried after a minute on failure, refreshed every 10 min while idle.
  (function baselineLoop() {
    const run = traffic ? Promise.resolve() : runBaseline(d);
    void run.finally(() => setTimeout(baselineLoop, baseline.error ? 60_000 : BASELINE_EVERY_MS));
  })();
  // Open the Kafka connection early so the first Start does not pay the TLS + IAM handshake.
  if (!d.produce) void producer(d).catch((e) => console.warn('[kafka] connect deferred:', String(e).slice(0, 200)));
}
