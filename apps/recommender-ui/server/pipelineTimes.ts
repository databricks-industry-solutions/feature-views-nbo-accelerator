// ---------------------------------------------------------------------------
// The pipeline's own timestamps for a Lakeshore Bank click. Each streaming online table in Lakebase records,
// per row, the event_time it aggregated, when Real-Time Mode emitted the value (output_time), and when it was
// written to Lakebase (commit_time). The Streaming Feature Views card shows those instead of inferring the
// pipeline time from the app's reads. Needs a Postgres role for the app service principal with SELECT on the
// online tables; without it the card shows the app-measured times only.
// ---------------------------------------------------------------------------
import { createLakebasePool } from '@databricks/lakebase';

type Pool = ReturnType<typeof createLakebasePool>;
export interface PipelineTimes {
  clicks?: { output_ms: number | null; commit_ms: number }; // cust_clicks_10m row for this click (epoch ms)
  cat30d?: { commit_ms: number }; // cust_cat_views_30d row for this click's category
  note?: string; // why the click-count row could not be read for this click
}
export interface PipelineTimesDeps {
  api: <T>(method: string, path: string) => Promise<T>; // app service principal
  endpoint: string; // projects/<online store>/branches/production/endpoints/primary
  catalog: string; // FE syncs each catalog's online tables into a Postgres database of the same name
  schema: string;
}

const RESOLVE_MS = 5 * 60_000;
let deps: PipelineTimesDeps | null = null;
let pool: Promise<Pool> | null = null;
let tables: { at: number; clicks?: string; cat30d?: string; mobile?: string } | null = null;
let lastError: string | null = null;

export function startPipelineTimes(d: PipelineTimesDeps) {
  deps = d;
}
export const pipelineTimesError = () => lastError;

async function getPool(d: PipelineTimesDeps): Promise<Pool> {
  pool ??= (async () => {
    const ep = await d.api<{ status?: { hosts?: { host?: string } } }>('GET', `/api/2.0/postgres/${d.endpoint}`);
    const host = ep.status?.hosts?.host;
    if (!host) throw new Error(`Lakebase endpoint ${d.endpoint} has no host`);
    return createLakebasePool({ endpoint: d.endpoint, host, database: d.catalog, max: 2, idleTimeoutMillis: 5 * 60_000 });
  })();
  pool.catch(() => { pool = null; });
  return pool;
}

// Postgres table of the streaming (Real-Time Mode) online materialization of a feature.
async function streamingTable(d: PipelineTimesDeps, feature: string): Promise<string | undefined> {
  const r = await d.api<{ materialized_features?: { table_name?: string; streaming_mode?: unknown; is_online?: boolean }[] }>(
    'GET', `/api/2.0/feature-engineering/materialized-features?feature_name=${encodeURIComponent(`${d.catalog}.${d.schema}.${feature}`)}`);
  const m = (r.materialized_features ?? []).find((x) => x.streaming_mode && x.is_online && x.table_name);
  return m?.table_name?.split('.').pop();
}

const ident = (s: string) => `"${s.replace(/"/g, '""')}"`;

const WATCH_EVERY_MS = 3;

/** Open the Lakebase connection and resolve the online tables ahead of the first click: opening it (endpoint
 *  lookup, token, TLS) takes longer than a click's watch, so a cold first click would miss its row. Called at
 *  startup and every 30 s while someone is using the app. */
export async function warmPipelineTimes(): Promise<void> {
  const d = deps;
  if (!d) return;
  try {
    if (!tables || Date.now() - tables.at > RESOLVE_MS) {
      tables = { at: Date.now(), clicks: await streamingTable(d, 'cust_clicks_10m'), cat30d: await streamingTable(d, 'cust_cat_views_30d'), mobile: await streamingTable(d, 'cust_mobile_cat_views_10m') };
    }
    await (await getPool(d)).query('SELECT 1');
    lastError = null;
  } catch (e) {
    lastError = String(e).slice(0, 300);
    tables = null;
  }
}

/** Watch the online tables from the Kafka write until each holds this click, and keep the pipeline timestamps
 *  the moment it does. Each table keeps one row per key, and any later write for the visitor (the next event,
 *  or the rolling window expiring an older click) replaces it, so the row has to be caught while it is fresh.
 *  eventMs identifies the click in the click-count row; target30d is the 30-day count that includes it; until()
 *  is read on every pass, so the caller can end the watch early. */
export async function watchPipeline(customerId: string, category: string, eventMs: number, target30d: number, until: () => number): Promise<PipelineTimes | null> {
  const d = deps;
  if (!d) return null;
  try {
    if (!tables || Date.now() - tables.at > RESOLVE_MS) {
      tables = { at: Date.now(), clicks: await streamingTable(d, 'cust_clicks_10m'), cat30d: await streamingTable(d, 'cust_cat_views_30d'), mobile: await streamingTable(d, 'cust_mobile_cat_views_10m') };
    }
    const p = await getPool(d);
    const s = ident(d.schema);
    const t = tables;
    const out: PipelineTimes = {};
    let lastSeenEvent: number | null | undefined;
    while (Date.now() < until() && (!out.clicks && t.clicks || !out.cat30d && t.cat30d)) {
      await Promise.all([
        !out.clicks && t.clicks && p.query(
          // EXTRACT(EPOCH ...) reads the stored UTC timestamps as epoch seconds, independent of the session time zone.
          `SELECT EXTRACT(EPOCH FROM event_time) * 1000 AS event_ms, EXTRACT(EPOCH FROM output_time) * 1000 AS output_ms,
                  EXTRACT(EPOCH FROM commit_time) * 1000 AS commit_ms
           FROM ${s}.${ident(t.clicks)} WHERE customer_id = $1`, [customerId]).then(({ rows }) => {
          const r = rows[0] as { event_ms: string | null; output_ms: string | null; commit_ms: string | null } | undefined;
          lastSeenEvent = r?.event_ms == null ? null : Number(r.event_ms);
          if (r?.commit_ms != null && lastSeenEvent != null && Math.abs(lastSeenEvent - eventMs) < 1) {
            out.clicks = { output_ms: r.output_ms == null ? null : Number(r.output_ms), commit_ms: Number(r.commit_ms) };
          }
        }),
        !out.cat30d && t.cat30d && p.query(
          `SELECT "cust_cat_views_30d-count" AS n, EXTRACT(EPOCH FROM commit_time) * 1000 AS commit_ms FROM ${s}.${ident(t.cat30d)}
           WHERE customer_id = $1 AND product_category = $2 ORDER BY commit_time DESC LIMIT 1`, [customerId, category]).then(({ rows }) => {
          const r = rows[0] as { n: string | null; commit_ms: string | null } | undefined;
          if (r?.commit_ms != null && Number(r.n) >= target30d && Number(r.commit_ms) >= eventMs - 1) out.cat30d = { commit_ms: Number(r.commit_ms) };
        }),
      ]);
      if (!out.clicks || !out.cat30d) await new Promise((r) => setTimeout(r, WATCH_EVERY_MS));
    }
    if (!out.clicks && t.clicks) {
      out.note = lastSeenEvent === null
        ? 'row was replaced by a window expiry before it could be read'
        : lastSeenEvent !== undefined && lastSeenEvent > eventMs ? 'row was replaced by a newer event for this visitor' : 'row did not show this click in time';
    }
    lastError = null;
    return out;
  } catch (e) {
    lastError = String(e).slice(0, 300);
    tables = null;
    return null;
  }
}

export interface FreshRow { event_ms: number; commit_ms: number }
/** Data freshness for Live stream's timed events, the README's metric: lag = commit_time - event_time, read from
 *  each key's row in the streaming online table. Every timed event has its own fresh key, so the row holds that
 *  event's first commit. Returns rows by customer_id (click count) and by "customer_id|category" (mobile views). */
export async function freshnessRows(clickKeys: string[], mobileKeys: { customer_id: string; category: string }[]):
  Promise<{ clicks: Map<string, FreshRow>; mobile: Map<string, FreshRow> } | null> {
  const d = deps;
  if (!d) return null;
  try {
    if (!tables || Date.now() - tables.at > RESOLVE_MS) {
      tables = { at: Date.now(), clicks: await streamingTable(d, 'cust_clicks_10m'), cat30d: await streamingTable(d, 'cust_cat_views_30d'), mobile: await streamingTable(d, 'cust_mobile_cat_views_10m') };
    }
    const p = await getPool(d);
    const s = ident(d.schema);
    const clicks = new Map<string, FreshRow>();
    const mobile = new Map<string, FreshRow>();
    const row = (r: { event_ms: string; commit_ms: string }) => ({ event_ms: Number(r.event_ms), commit_ms: Number(r.commit_ms) });
    await Promise.all([
      tables.clicks && clickKeys.length && p.query(
        `SELECT customer_id, EXTRACT(EPOCH FROM event_time) * 1000 AS event_ms, EXTRACT(EPOCH FROM commit_time) * 1000 AS commit_ms
         FROM ${s}.${ident(tables.clicks)} WHERE customer_id = ANY($1::text[]) AND event_time IS NOT NULL`, [clickKeys]).then(({ rows }) => {
        for (const r of rows as { customer_id: string; event_ms: string; commit_ms: string }[]) clicks.set(r.customer_id, row(r));
      }),
      tables.mobile && mobileKeys.length && p.query(
        `SELECT t.customer_id, t.product_category, EXTRACT(EPOCH FROM t.event_time) * 1000 AS event_ms, EXTRACT(EPOCH FROM t.commit_time) * 1000 AS commit_ms
         FROM ${s}.${ident(tables.mobile)} t JOIN unnest($1::text[], $2::text[]) AS k(c, p) ON t.customer_id = k.c AND t.product_category = k.p
         WHERE t.event_time IS NOT NULL`, [mobileKeys.map((k) => k.customer_id), mobileKeys.map((k) => k.category)]).then(({ rows }) => {
        for (const r of rows as { customer_id: string; product_category: string; event_ms: string; commit_ms: string }[]) mobile.set(`${r.customer_id}|${r.product_category}`, row(r));
      }),
    ]);
    lastError = null;
    return { clicks, mobile };
  } catch (e) {
    lastError = String(e).slice(0, 300);
    tables = null;
    return null;
  }
}

