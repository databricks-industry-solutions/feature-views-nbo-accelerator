// ---------------------------------------------------------------------------
// Real visitor clicks on the Lakeshore Bank tab. Each click is written to Kafka like live traffic, then the
// visitor's own key is read back through the Feature Serving endpoint (one row per request) until the
// streaming features show it. The panel on the Bank tab shows each click, the feature values before and
// after, and how long the click took to become servable.
// ---------------------------------------------------------------------------
import { randomUUID } from 'node:crypto';
import type { ClickEvent } from './stream.js';

export interface FeatureValues {
  cust_clicks_10m?: number;
  cust_cat_views_30d?: number;
}
export interface ClickDeps {
  produce: (events: ClickEvent[]) => Promise<void>;
  read: (customerId: string, category: string) => Promise<{ values: FeatureValues; readAt: number }>;
  live: () => boolean; // ingestion + Real-Time Mode pipelines are running
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
  servable_ms?: number; // event_time -> first Feature Serving read that returned the click
  error?: string;
}

const POLL_MS = 50;
const TIMEOUT_MS = 15_000;
const KEEP_PER_SESSION = 20;
const SESSION_IDLE_MS = 30 * 60_000;

let deps: ClickDeps | null = null;
const sessions = new Map<string, { clicks: TrackedClick[]; chain: Promise<void>; seen: number }>();

export function startClicks(d: ClickDeps) {
  deps = d;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function follow(d: ClickDeps, c: TrackedClick, event: ClickEvent) {
  try {
    c.before = (await d.read(c.customer_id, c.category)).values;
    await d.produce([event]);
    const eventMs = Date.parse(event.event_time);
    const expect = (c.before.cust_clicks_10m ?? 0) + 1;
    const until = Date.now() + (d.timeoutMs ?? TIMEOUT_MS);
    while (Date.now() < until) {
      const { values, readAt } = await d.read(c.customer_id, c.category);
      if ((values.cust_clicks_10m ?? 0) >= expect) {
        c.after = values;
        c.servable_ms = Math.max(0, Math.round(readAt - eventMs));
        c.status = 'served';
        return;
      }
      await sleep(d.pollMs ?? POLL_MS);
    }
    c.status = 'missing';
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
  const event: ClickEvent = {
    event_id: `web_${id}`, customer_id: customerId, event_time: new Date().toISOString(),
    event_type: eventType, product_category: category, dwell_ms: 0, device: 'web',
  };
  s.chain = s.chain.then(() => follow(d, c, event));
  return c;
}

export function sessionClicks(session: string): TrackedClick[] {
  const s = sessions.get(session);
  if (s) s.seen = Date.now();
  return s ? [...s.clicks].reverse() : [];
}
