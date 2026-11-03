// ---------------------------------------------------------------------------
// Real visitor clicks on the Lakeshore Bank tab. Each click is written to Kafka like live traffic, then the
// visitor's own key is read back through the Feature Serving endpoint (one row per request) until the
// streaming features show it. The panel on the Bank tab shows each click, the feature values before and
// after, and how long the click took to become servable.
// ---------------------------------------------------------------------------
import { randomUUID } from 'node:crypto';
import type { PipelineTimes } from './pipelineTimes.js';
import type { ClickEvent } from './stream.js';

export interface FeatureValues {
  cust_clicks_10m?: number;
  cust_cat_views_30d?: number;
  cust_mobile_cat_views_10m?: number;
}
export interface ClickDeps {
  produce: (events: ClickEvent[]) => Promise<void>;
  // Resolves the Feature Serving endpoint once per click and returns a one-row reader for it; latencyMs is the
  // data-plane request alone (no endpoint lookup or token mint).
  reader: () => Promise<(customerId: string, category: string) => Promise<{ values: FeatureValues; latencyMs: number }>>;
  live: () => boolean; // ingestion + Real-Time Mode pipelines are running
  // Watches the online tables from the Kafka write and returns the pipeline's own timestamps for this click;
  // null when Lakebase is not readable.
  pipeline?: (customerId: string, category: string, eventMs: number, target30d: number, until: () => number) => Promise<PipelineTimes | null>;
  pollMs?: number;
  timeoutMs?: number;
}
export interface TrackedClick {
  id: string;
  customer_id: string;
  event_type: string;
  category: string;
  sent_at: number;
  status: 'pending' | 'served' | 'missing' | 'off' | 'error';
  before?: FeatureValues;
  after?: FeatureValues;
  // Timing, every value in ms after event_time (stamped by the app just before the Kafka write):
  kafka_ms?: number; // app: Kafka acknowledged the write on all replicas
  rtm_ms?: number; // pipeline: Real-Time Mode emitted the new click count (online table output_time)
  lakebase_ms?: number; // pipeline: the new click count was written to Lakebase (online table commit_time)
  total_ms?: number; // app: Feature Serving returned every updated feature
  serving_ms?: number; // app: duration of the Feature Serving request that returned the last one
  // Per streaming feature: written to Lakebase (commit_time) and returned by Feature Serving (first read that had it).
  lakebase_feature_ms?: Partial<Record<keyof FeatureValues, number>>;
  pipeline_note?: string; // why the pipeline timestamps are missing, when they are
  // Per streaming feature: event_time -> the first read that returned its new value. Each feature is its own
  // Real-Time Mode pipeline, so they land at different times; a feature missing here did not move in time.
  feature_ms?: Partial<Record<keyof FeatureValues, number>>;
  error?: string;
}

// While waiting, a new read starts every POLL_MS (at most MAX_IN_FLIGHT at once), so the first read that returns
// the new value started within about POLL_MS of the value landing in the online store.
const POLL_MS = 5;
const MAX_IN_FLIGHT = 8;
// After the first feature lands, wait this long for the click's other features before reporting.
const OTHERS_MS = 3_000;
// A website click moves the visitor's click count and that category's 30-day views. The mobile feature only
// counts mobile-app events, so a website click leaves it unchanged.
const EXPECTED: (keyof FeatureValues)[] = ['cust_clicks_10m', 'cust_cat_views_30d'];
const TIMEOUT_MS = 15_000;
const KEEP_PER_SESSION = 20;
const SESSION_IDLE_MS = 30 * 60_000;

let deps: ClickDeps | null = null;
const sessions = new Map<string, { clicks: TrackedClick[]; chain: Promise<void>; seen: number }>();

export function startClicks(d: ClickDeps) {
  deps = d;
}

async function follow(d: ClickDeps, c: TrackedClick) {
  try {
    const read = await d.reader();
    c.before = (await read(c.customer_id, c.category)).values;
    // Stamp the event after the "before" read, so the timing starts at the Kafka write.
    const eventMs = Date.now();
    const event: ClickEvent = {
      event_id: `web_${c.id}`, customer_id: c.customer_id, event_time: new Date(eventMs).toISOString(),
      event_type: c.event_type, product_category: c.category, dwell_ms: 0, device: 'web',
    };
    await d.produce([event]);
    const ackMs = Date.now();
    c.kafka_ms = ackMs - eventMs;
    const target = Object.fromEntries(EXPECTED.map((k) => [k, (c.before?.[k] ?? 0) + 1])) as Record<keyof FeatureValues, number>;
    const until = ackMs + (d.timeoutMs ?? TIMEOUT_MS);
    // Watch Lakebase from now, alongside the Feature Serving reads, so each row is read while it still holds
    // this click (a later write for the visitor replaces it).
    // It ends 200 ms after the reads finish, so a click whose row cannot be matched does not hold up the next one.
    let watchUntil = ackMs + OTHERS_MS + 2_000;
    const watch = d.pipeline?.(c.customer_id, c.category, eventMs, target.cust_cat_views_30d, () => watchUntil);
    await new Promise<void>((resolve) => {
      let inFlight = 0;
      let firstAt = 0; // when the first expected feature was seen
      let lastError = '';
      // Per feature: the earliest-started read that returned its new value.
      const hits: Partial<Record<keyof FeatureValues, { start: number; end: number; latencyMs: number; values: FeatureValues }>> = {};
      const allIn = () => EXPECTED.every((k) => hits[k]);
      const stopReading = () => allIn() || Date.now() > until || (firstAt > 0 && Date.now() - firstAt > OTHERS_MS);
      const finish = () => {
        clearInterval(timer);
        const got = EXPECTED.filter((k) => hits[k]);
        if (!got.length) {
          c.status = lastError ? 'error' : 'missing';
          if (lastError) c.error = lastError;
          resolve();
          return;
        }
        // The read that completed the last feature gives the values shown and the timing split.
        const last = got.map((k) => hits[k]!).reduce((a, b) => (b.end > a.end ? b : a));
        const servingStart = Math.max(ackMs, last.end - last.latencyMs);
        c.after = last.values;
        c.feature_ms = Object.fromEntries(got.map((k) => [k, hits[k]!.end - eventMs]));
        c.serving_ms = last.end - servingStart;
        c.total_ms = last.end - eventMs;
        c.status = 'served';
        resolve();
      };
      const timer = setInterval(() => {
        if (inFlight >= MAX_IN_FLIGHT) return;
        if (stopReading()) {
          if (inFlight === 0) finish();
          return;
        }
        const start = Date.now();
        inFlight += 1;
        read(c.customer_id, c.category).then(({ values, latencyMs }) => {
          const end = Date.now();
          for (const k of EXPECTED) {
            const h = hits[k];
            if ((values[k] ?? 0) >= target[k] && (!h || start < h.start)) hits[k] = { start, end, latencyMs, values };
          }
          if (!firstAt && EXPECTED.some((k) => hits[k])) firstAt = end;
        }, (e) => { lastError = String(e).slice(0, 200); }).finally(() => {
          inFlight -= 1;
          // Wait for reads already in flight: one that started earlier may also have the new values.
          if (inFlight === 0 && stopReading()) finish();
        });
      }, d.pollMs ?? POLL_MS);
    });
    watchUntil = Math.min(watchUntil, Date.now() + 200);
    const t = watch ? await watch : null;
    if (c.status === 'served' && d.pipeline) {
      if (t?.clicks) {
        if (t.clicks.output_ms != null) c.rtm_ms = Math.round(t.clicks.output_ms - eventMs);
        c.lakebase_ms = Math.round(t.clicks.commit_ms - eventMs);
      }
      const lb: Partial<Record<keyof FeatureValues, number>> = {};
      if (t?.clicks) lb.cust_clicks_10m = Math.round(t.clicks.commit_ms - eventMs);
      if (t?.cat30d) lb.cust_cat_views_30d = Math.round(t.cat30d.commit_ms - eventMs);
      if (Object.keys(lb).length) c.lakebase_feature_ms = lb;
      c.pipeline_note = t ? t.note : 'Lakebase timestamps unavailable';
    }
  } catch (e) {
    c.status = 'error';
    c.error = String(e).slice(0, 200);
  }
}

/** Record one Bank-tab click. Clicks from one session are followed one at a time, so each before/after pair
 *  belongs to that click alone. */
export function trackClick(session: string, customerId: string, eventType: string, category: string): TrackedClick {
  const d = deps;
  if (!d) throw new Error('clicks module not started');
  const now = Date.now();
  for (const [k, s] of sessions) if (now - s.seen > SESSION_IDLE_MS) sessions.delete(k);
  const s = sessions.get(session) ?? { clicks: [], chain: Promise.resolve(), seen: now };
  s.seen = now;
  sessions.set(session, s);

  const id = randomUUID().replace(/-/g, '');
  const c: TrackedClick = { id, customer_id: customerId, event_type: eventType, category, sent_at: now, status: 'pending' };
  s.clicks.push(c);
  if (s.clicks.length > KEEP_PER_SESSION) s.clicks.splice(0, s.clicks.length - KEEP_PER_SESSION);
  if (!d.live()) {
    // With ingestion off the event would never be read (ingestion starts from the latest offset), so skip it.
    c.status = 'off';
    return c;
  }
  s.chain = s.chain.then(() => follow(d, c));
  return c;
}

export function sessionClicks(session: string): TrackedClick[] {
  const s = sessions.get(session);
  if (s) s.seen = Date.now();
  return s ? [...s.clicks].reverse() : [];
}
