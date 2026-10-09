import { createApp, analytics, server } from '@databricks/appkit';
import { metrics, trafficStatus, trafficStart, trafficEnsure, trafficStop, type LiveCfg } from './realtime.js';
import { assertRecommendationGoal, constrainOffersForContext, constrainOffersForGoal } from '../shared/recommendation.js';

// ---------------------------------------------------------------------------
// In-app NBO assistant. The agent loop runs HERE, in the app's own server (no
// separate agent endpoint).
//
// Model (MODEL_MODE): 'provider' = Claude on the customer's Anthropic
// subscription via a UC AI Gateway provider service; 'fm' = hosted Databricks FM Claude.
// The model call always runs as the app service principal (client-credentials).
//
// SQL: read-only queries on the app's SQL warehouse, executed ON BEHALF OF the
// signed-in analyst (x-forwarded-access-token) so UC row/column security applies
// per user; falls back to the SP token for local dev.
// ---------------------------------------------------------------------------

// Databricks Apps injects DATABRICKS_HOST as a bare hostname (no scheme); local .env has the
// full https:// URL. Normalize so `${HOST}/oidc/...` / `${HOST}/serving-endpoints/...` are always
// parseable — a schemeless value makes fetch() throw "Failed to parse URL".
const HOST = (() => {
  let h = (process.env.DATABRICKS_HOST ?? '').trim().replace(/\/+$/, '');
  if (h && !/^https?:\/\//i.test(h)) h = `https://${h}`;
  return h;
})();
const WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID ?? '';
const MODEL_MODE = process.env.MODEL_MODE ?? 'fm'; // 'provider' | 'fm'
const PROVIDER_SVC = process.env.MODEL_PROVIDER_SERVICE ?? '';
const AGENT_MODEL = process.env.AGENT_MODEL ?? 'databricks-claude-sonnet-5';
const CATALOG = process.env.CATALOG ?? 'fins_industry_solutions';
const SCHEMA = process.env.SCHEMA ?? 'nbo';
// Route-optimized online-lookup ranker: request carries customer_id + offer fields; the endpoint
// fetches the customer's features from the online store by key and returns P(accept) per offer.
const RANKER_ENDPOINT = process.env.RANKER_ENDPOINT ?? 'nbo-ranker-realtime';
// Feature Serving endpoint over the nbo_customer_profile FeatureSpec (notebook 10): the visitor's stored,
// streaming (Rolling + Sawtooth), and CustomUDF features, used for the "signals behind this match" panel
// and to measure streaming freshness.
const FEATURE_ENDPOINT = process.env.FEATURE_ENDPOINT ?? 'nbo-customer-features';
// "Simulate live traffic": the notebook 08 continuous Kafka producer, bound to the app as a job resource.
const TRAFFIC_JOB_ID = process.env.TRAFFIC_JOB_ID ?? '';
const CATEGORIES = ['credit_card', 'savings', 'personal_loan', 'mortgage', 'investment'];
const FQ = `\`${CATALOG}\`.${SCHEMA}`;

const SYSTEM = `You are the Next-Best-Offer assistant for a retail bank, embedded in a
recommendation app running on Databricks. You explain, in plain language for a
relationship banker, WHY the ranking model recommends a given offer to a given
customer — and answer follow-ups about that customer and the offer catalog.

Be concise and specific. Lead with a one-sentence answer. Use the customer's
profile (loyalty tier, risk band, income, tenure) and the offer's attributes
(category, base reward, tier requirement) in your reasoning. When a numeric fact
would help and you're unsure, call the run_sql tool against the tables below.
Never mention SQL or internal mechanics unless asked. Do not invent numbers.

Rules:
- READ-ONLY. Only SELECT. Never INSERT/UPDATE/DELETE/MERGE/CREATE/DROP/ALTER.
- Add LIMIT 1000 unless the query is an aggregate.
- Never invent columns or values. If the data can't answer, say so plainly.

Tables (Unity Catalog):
  ${FQ}.customers  -- customer_id, loyalty_tier, risk_band, annual_income, tenure_months
  ${FQ}.offers     -- offer_id, product_category, offer_text, base_reward, tier_requirement
  ${FQ}.labels     -- record_id, customer_id, offer_id, ts, accepted (0/1 historical acceptance)

Domain notes:
- product_category ∈ {credit_card, savings, personal_loan, mortgage, investment}.
- The ranker returns P(accept) per offer; higher-tier / higher-income customers
  clear higher tier_requirement offers and tend to accept higher-reward products.
- "acceptance rate" for an offer = AVG(accepted) over ${FQ}.labels.`;

const TOOLS = [
  {
    name: 'run_sql',
    description: 'Execute one read-only SELECT and return rows as JSON.',
    input_schema: {
      type: 'object',
      properties: { query: { type: 'string', description: 'A single SELECT statement.' } },
      required: ['query'],
    },
  },
];

interface ContentBlock {
  type: string;
  text?: string;
  id?: string;
  name?: string;
  input?: Record<string, unknown>;
}
interface SqlResp {
  statement_id?: string;
  status?: { state?: string; error?: unknown };
  manifest?: { schema?: { columns?: { name: string }[] } };
  result?: { data_array?: (string | null)[][] };
}
interface ChatBody {
  message?: string;
  history?: { role?: string; content?: string }[];
  context?: string; // optional per-offer/customer context injected into the system prompt
}

let cachedToken: { token: string; exp: number } | null = null;

async function getToken(): Promise<string> {
  if (process.env.DATABRICKS_TOKEN) return process.env.DATABRICKS_TOKEN;
  const now = Date.now();
  if (cachedToken && cachedToken.exp > now + 60_000) return cachedToken.token;
  const id = process.env.DATABRICKS_CLIENT_ID;
  const secret = process.env.DATABRICKS_CLIENT_SECRET;
  if (id && secret) {
    // Deployed app / SP: OAuth client-credentials.
    const basic = Buffer.from(`${id}:${secret}`).toString('base64');
    const r = await fetch(`${HOST}/oidc/v1/token`, {
      method: 'POST',
      headers: { Authorization: `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: 'grant_type=client_credentials&scope=all-apis',
    });
    if (!r.ok) throw new Error(`token mint failed: ${r.status} ${await r.text()}`);
    const j = (await r.json()) as { access_token: string; expires_in: number };
    cachedToken = { token: j.access_token, exp: now + j.expires_in * 1000 };
    return j.access_token;
  }
  // Local-dev fallback: mint a short-lived OAuth token from the Databricks CLI
  // using the configured profile (DATABRICKS_CONFIG_PROFILE). Never hit in the
  // deployed app, which always has OBO (x-forwarded-access-token) or SP creds.
  try {
    const { execFileSync } = await import('node:child_process');
    const args = ['auth', 'token'];
    const profile = process.env.DATABRICKS_CONFIG_PROFILE;
    if (profile) args.push('-p', profile);
    const out = execFileSync('databricks', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
    const j = JSON.parse(out) as { access_token: string; expiry?: string };
    const exp = j.expiry ? Date.parse(j.expiry) : now + 50 * 60_000;
    cachedToken = { token: j.access_token, exp };
    return j.access_token;
  } catch (e) {
    throw new Error(
      'No DATABRICKS_TOKEN, no client credentials, and CLI token mint failed. ' +
        'For local dev set DATABRICKS_CONFIG_PROFILE to a logged-in profile. ' +
        String(e),
    );
  }
}

// Write/DDL keywords. `replace` is intentionally NOT here: it's a common read-only string
// function (replace(col,…)), and the only dangerous form, CREATE OR REPLACE, is caught by `create`.
const BLOCKED = /\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke)\b/i;

// Single source of truth: submit a statement to the SQL warehouse, poll to completion,
// return columns + parsed row objects. runSql (LLM-facing string) and queryRows (typed rows)
// both format from this.
async function executeStatement(statement: string, token: string): Promise<Record<string, unknown>[]> {
  const auth = { Authorization: `Bearer ${token}` };
  const res = await fetch(`${HOST}/api/2.0/sql/statements/`, {
    method: 'POST',
    headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      warehouse_id: WAREHOUSE_ID, statement, wait_timeout: '30s',
      on_wait_timeout: 'CANCEL', format: 'JSON_ARRAY', disposition: 'INLINE',
    }),
  });
  if (!res.ok) throw new Error(`SQL ${res.status}: ${await res.text()}`);
  let data = (await res.json()) as SqlResp;
  let guard = 0;
  while (data.status?.state && ['PENDING', 'RUNNING'].includes(data.status.state) && guard < 30) {
    guard += 1;
    await new Promise((r) => setTimeout(r, 500));
    const p = await fetch(`${HOST}/api/2.0/sql/statements/${data.statement_id}`, { headers: auth });
    data = (await p.json()) as SqlResp;
  }
  if (data.status?.state !== 'SUCCEEDED') {
    throw new Error(`SQL ${data.status?.state ?? 'FAILED'}: ${JSON.stringify(data.status?.error ?? {})}`);
  }
  const cols = (data.manifest?.schema?.columns ?? []).map((c) => c.name);
  const rows = (data.result?.data_array ?? []).slice(0, 1000);
  return rows.map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i]])));
}

// LLM tool wrapper: guard read-only, run, and return a compact JSON string.
async function runSql(statement: string, token: string): Promise<string> {
  const q = statement.trim().replace(/;+\s*$/, '');
  if (!/^\s*(select|with)\b/i.test(q)) return 'ERROR: only read-only SELECT queries are allowed.';
  if (BLOCKED.test(q)) return 'ERROR: statement contains a write keyword; only SELECT is allowed.';
  try {
    const out = await executeStatement(q, token);
    return JSON.stringify({ row_count: out.length, rows: out }).slice(0, 8000);
  } catch (e) {
    return `ERROR running query: ${String(e)}`;
  }
}

// App data endpoints: parsed rows.
async function queryRows(statement: string, token: string): Promise<Record<string, unknown>[]> {
  return executeStatement(statement, token);
}

// Route-optimized endpoints require an OAuth token DOWNSCOPED to the endpoint (query_inference_endpoint).
// A plain identity token is rejected. Mint one via client_credentials + authorization_details using the
// app service principal (DATABRICKS_CLIENT_ID/SECRET, injected in the deployed app).
// Endpoint-downscoped query tokens are reusable until they expire, so cache per endpoint id and
// reuse (with a 60s safety margin) instead of minting a fresh one on every /api/recommend — the
// mint is a synchronous OAuth round-trip on this app's advertised sub-300ms hot path.
const queryTokens = new Map<string, { token: string; exp: number }>();
async function mintQueryToken(endpointId: string): Promise<string> {
  const now = Date.now();
  const hit = queryTokens.get(endpointId);
  if (hit && hit.exp > now + 60_000) return hit.token;
  // Prefer a dedicated query SP (QUERY_SP_CLIENT_ID/SECRET, injected from the `nbo` secret scope).
  // Databricks App service principals are not permitted to use the authorization_details
  // downscoping flow that route-optimized endpoints require, so the app's own SP creds don't work
  // here — a regular SP with CAN_QUERY does (the same SP nb06/nb10 use).
  // Only the dedicated query SP works here. Do NOT fall back to the app SP
  // (DATABRICKS_CLIENT_ID/SECRET): a Databricks App SP is not permitted to use the
  // authorization_details downscoping flow, so falling back would just produce an opaque
  // invalid_authorization_details 400 instead of this actionable message.
  const id = process.env.QUERY_SP_CLIENT_ID;
  const secret = process.env.QUERY_SP_CLIENT_SECRET;
  if (!id || !secret) throw new Error('Route-optimized query needs a query service principal with CAN_QUERY on the endpoint. Set QUERY_SP_CLIENT_ID/QUERY_SP_CLIENT_SECRET (injected from the nbo secret scope). The app\'s own SP cannot mint the endpoint-downscoped token.');
  const basic = Buffer.from(`${id}:${secret}`).toString('base64');
  const authz = JSON.stringify([{
    type: 'workspace_permission', object_type: 'serving-endpoints',
    object_path: `/serving-endpoints/${endpointId}`, actions: ['query_inference_endpoint'],
  }]);
  const r = await fetch(`${HOST}/oidc/v1/token`, {
    method: 'POST',
    headers: { Authorization: `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'client_credentials', scope: 'all-apis', authorization_details: authz }),
  });
  if (!r.ok) throw new Error(`downscoped token mint failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  const j = (await r.json()) as { access_token: string; expires_in?: number };
  queryTokens.set(endpointId, { token: j.access_token, exp: now + (j.expires_in ?? 3600) * 1000 });
  return j.access_token;
}

// Drop the cached downscoped token so the next mintQueryToken() re-mints. Call after a 401/403 from
// the ranker (e.g. the SP's CAN_QUERY grant was revoked, or the token was server-side invalidated)
// so a stale cached token doesn't keep the app broken until its natural expiry.
function invalidateQueryToken(endpointId: string): void {
  queryTokens.delete(endpointId);
}

interface EndpointInfo { url: string; id: string }
// Resolved on every use, never cached: the endpoint id and data-plane URL both change when an
// endpoint is recreated, and a stale value would route every later call to a dead URL.
async function getEndpoint(name: string, token: string): Promise<EndpointInfo> {
  const r = await fetch(`${HOST}/api/2.0/serving-endpoints/${name}`, { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) throw new Error(`get endpoint ${name} failed: ${r.status} ${(await r.text()).slice(0, 200)}`);
  const d = (await r.json()) as { id?: string; data_plane_info?: { query_info?: { endpoint_url?: string } } };
  const url = d.data_plane_info?.query_info?.endpoint_url;
  if (!url || !d.id) throw new Error(`endpoint ${name} is not route-optimized (no data-plane query URL).`);
  return { url, id: d.id };
}

// Query a route-optimized endpoint. The token mint and endpoint lookup happen BEFORE the timer, so
// latency_ms is only the data-plane round trip. On 401/403 the cached token is re-minted once.
async function queryEndpoint(name: string, records: Record<string, unknown>[]): Promise<{ json: Record<string, unknown>; latencyMs: number }> {
  const ep = await getEndpoint(name, await getToken());
  const body = JSON.stringify({ dataframe_records: records });
  const post = (t: string) => fetch(ep.url, { method: 'POST', headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' }, body });
  let token = await mintQueryToken(ep.id);
  let t0 = Date.now();
  let r = await post(token);
  if (r.status === 401 || r.status === 403) {
    invalidateQueryToken(ep.id);
    token = await mintQueryToken(ep.id);
    t0 = Date.now();
    r = await post(token);
  }
  // After an idle period the endpoint's feature-lookup client can reuse Lakebase connections the server already
  // closed ("SSL error: unexpected eof" / "connection is lost"); each serving worker fails once, then reconnects.
  // Retry only that transient error, up to twice, and report the latency of the call that succeeded.
  for (let attempt = 0; r.status === 400 && attempt < 2; attempt += 1) {
    const text = await r.clone().text();
    if (!/SSL error|connection is lost|server closed the connection/i.test(text)) break;
    console.warn(`[${name}] transient lookup connection error, retrying (${attempt + 1}/2)`);
    t0 = Date.now();
    r = await post(token);
  }
  const latencyMs = Date.now() - t0;
  if (!r.ok) throw new Error(`${name} query failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  return { json: (await r.json()) as Record<string, unknown>, latencyMs };
}


// Visitor id: signed-in visitors use their customer_id; guests get a stable id per browser session,
// so their clicks still build streaming features (keyed by that id) that the ranker looks up.
const visitorId = (customerId: unknown, sessionId: unknown) =>
  customerId ? String(customerId) : `guest_${String(sessionId ?? 'anon').replace(/[^a-zA-Z0-9]/g, '').slice(0, 12)}`;

// OfferMatch answers + the visitor's website clicks per category in the last 10 minutes of this session.
interface Ctx {
  goal?: string;
  credit?: string;
  income?: number;
  card_spend?: number;
  credit_monthly_spend?: number;
  savings_deposit?: number;
  loan_amount?: number;
  loan_term_months?: number;
  home_price?: number;
  mortgage_down_payment_pct?: number;
  investment_amount?: number;
  investment_monthly_contribution?: number;
  session_cat_views?: Record<string, number>;
}
const ctxColumns = (c: Ctx = {}) => ({
  ctx_goal: CATEGORIES.includes(String(c.goal)) ? String(c.goal) : 'none',
  ctx_credit: ['excellent', 'good', 'fair'].includes(String(c.credit)) ? String(c.credit) : 'good',
  ctx_income: Number.isFinite(Number(c.income)) && Number(c.income) > 0 ? Number(c.income) : 85000,
  ctx_card_spend: Number.isFinite(Number(c.card_spend)) && Number(c.card_spend) >= 0 ? Number(c.card_spend) : 1500,
});

// Offer catalog is static demo data: cache it briefly so the hot path is one endpoint call.
interface OfferRow extends Record<string, unknown> {
  offer_id: unknown;
  product_category: unknown;
  offer_text: unknown;
  base_reward: unknown;
  tier_requirement: unknown;
}
let offersCache: { rows: OfferRow[]; exp: number } | null = null;
async function getOffers(token: string): Promise<OfferRow[]> {
  if (offersCache && offersCache.exp > Date.now()) return offersCache.rows;
  const rows = (await queryRows(
    `SELECT offer_id, product_category, offer_text, base_reward, tier_requirement FROM ${FQ}.offers ORDER BY offer_id`,
    token,
  )) as OfferRow[];
  offersCache = { rows, exp: Date.now() + 5 * 60_000 };
  return rows;
}

async function rankAll(customerId: string, ctx: Ctx, token: string) {
  const cc = ctxColumns(ctx);
  const allOffers = await getOffers(token);
  // OfferMatch is a product-intent form, not a weak preference hint. If a visitor explicitly asks
  // for a mortgage, recommending a credit card is semantically wrong. Constrain candidates to the
  // selected category, then let the model personalize WHICH product in that category is best.
  // With no selected goal the full catalog remains eligible.
  const { offers: goalOffers, requestedGoal } = constrainOffersForGoal(allOffers, cc.ctx_goal);
  const views = ctx.session_cat_views ?? {};
  // ctx_session_cat_views is per offer row: the visitor's clicks in THAT offer's category, so a click on
  // mortgages moves only the mortgage rows.
  const sessionViews = (cat: unknown) => {
    const v = Math.floor(Number(views[String(cat)] ?? 0));
    return Number.isFinite(v) && v > 0 ? Math.min(v, 50) : 0;
  };
  const records = allOffers.map((o) => ({
    customer_id: customerId, offer_id: o.offer_id, product_category: o.product_category,
    base_reward: Number(o.base_reward), tier_requirement: Number(o.tier_requirement), ...cc,
    ctx_session_cat_views: sessionViews(o.product_category),
  }));
  const { json, latencyMs } = await queryEndpoint(RANKER_ENDPOINT, records);
  const preds = json.predictions;
  // Expect a flat numeric array aligned 1:1 with offers; anything else would silently mis-sort.
  const nums = Array.isArray(preds) ? preds.map(Number) : [];
  if (nums.length !== allOffers.length || nums.some((n) => !Number.isFinite(n))) {
    throw new Error(`ranker returned an unexpected predictions shape: expected ${allOffers.length} numeric scores, got ${JSON.stringify(preds).slice(0, 200)}`);
  }
  const scoredAll = allOffers.map((o, i) => ({ ...o, score: nums[i] }) as OfferRow & { score: number })
    .sort((a, b) => b.score - a.score);
  const goalIds = new Set(goalOffers.map((o) => String(o.offer_id)));
  const scoredGoal = scoredAll.filter((o) => goalIds.has(String(o.offer_id)));
  const scored = constrainOffersForContext(scoredGoal, {
    credit: cc.ctx_credit as 'excellent' | 'good' | 'fair',
    income: cc.ctx_income,
    card_spend: cc.ctx_card_spend,
    credit_monthly_spend: ctx.credit_monthly_spend,
    savings_deposit: ctx.savings_deposit,
    loan_amount: ctx.loan_amount,
    loan_term_months: ctx.loan_term_months,
    home_price: ctx.home_price,
    mortgage_down_payment_pct: ctx.mortgage_down_payment_pct,
    investment_amount: ctx.investment_amount,
    investment_monthly_contribution: ctx.investment_monthly_contribution,
  });
  assertRecommendationGoal(scored[0]?.product_category, requestedGoal);
  return { scored, latencyMs };
}

// One Feature Serving lookup: a row per category, so category-keyed features come back per row.
async function lookupProfile(customerId: string) {
  const { json, latencyMs } = await queryEndpoint(FEATURE_ENDPOINT, CATEGORIES.map((c) => ({ customer_id: customerId, product_category: c })));
  const rows = (Array.isArray(json.outputs) ? json.outputs : Array.isArray(json.predictions) ? json.predictions : []) as Record<string, unknown>[];
  const num = (v: unknown) => (v === null || v === undefined || Number.isNaN(Number(v)) ? undefined : Number(v));
  const first = rows[0] ?? {};
  const customer: Record<string, unknown> = {};
  for (const k of ['cust_loyalty_tier', 'cust_risk_band']) if (first[k] != null) customer[k] = first[k];
  for (const k of ['cust_annual_income', 'cust_tenure_months', 'cust_spend_90d', 'cust_avg_balance_30d', 'cust_spend_to_income', 'cust_clicks_10m']) {
    const v = num(first[k]); if (v !== undefined) customer[k] = v;
  }
  const categories: Record<string, { cust_mobile_cat_views_10m?: number; cust_cat_views_30d?: number }> = {};
  CATEGORIES.forEach((c, i) => {
    const r = rows[i] ?? {};
    categories[c] = { cust_mobile_cat_views_10m: num(r.cust_mobile_cat_views_10m) ?? 0, cust_cat_views_30d: num(r.cust_cat_views_30d) };
  });
  return { customer, categories, latencyMs };
}

// Jobs API calls run as the app's service principal (granted CAN_MANAGE_RUN on the traffic job through the
// app resource binding). Stream-stats SQL runs as the signed-in user when a forwarded token is present.
const liveCfg = (userToken?: string): LiveCfg => ({
  host: HOST, fq: FQ, catalog: CATALOG, schema: SCHEMA, rankerEndpoint: RANKER_ENDPOINT,
  trafficJobId: TRAFFIC_JOB_ID,
  getToken,
  query: async (sql: string) => queryRows(sql, userToken ?? (await getToken())),
});

// Demo accounts: real customers nearest to four archetypes (tier + risk exact, income/tenure closest).
const ARCHETYPES = [
  { key: 'maya', tier: 'bronze', risk: 'high', income: 48000, tenure: 6 },
  { key: 'raj', tier: 'gold', risk: 'low', income: 145000, tenure: 54 },
  { key: 'elena', tier: 'platinum', risk: 'low', income: 210000, tenure: 117 },
  { key: 'sam', tier: 'silver', risk: 'medium', income: 72000, tenure: 27 },
];
let accountsCache: Record<string, unknown>[] | null = null;
let pingCustomers: string[] = [];
let pingIndex = 0;
let latencyTestSession = 0;
let latencyTestRunning = false;

async function callModel(messages: unknown[], system: string): Promise<Response> {
  const token = await getToken();
  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    'anthropic-version': '2023-06-01',
  };
  let url: string;
  if (MODEL_MODE === 'provider') {
    url = `${HOST}/ai-gateway/anthropic/v1/messages`;
    headers['Databricks-Model-Provider-Service'] = PROVIDER_SVC;
  } else {
    // Databricks FM Claude speaks the native Anthropic Messages schema at this
    // gateway path (model in body) — returns content blocks + tool_use, which
    // the tool loop below expects. (/invocations returns OpenAI format instead.)
    url = `${HOST}/serving-endpoints/anthropic/v1/messages`;
  }
  return fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({ model: AGENT_MODEL, max_tokens: 1024, system, messages, tools: TOOLS }),
  });
}

createApp({
  plugins: [analytics(), server()],
  async onPluginsReady(appkit) {
    appkit.server.extend((app) => {
      // Lightweight config for the UI header (catalog/schema the app is pointed at).
      app.get('/api/config', (_req, res) => {
        res.json({ catalog: CATALOG, schema: SCHEMA, ranker_endpoint: RANKER_ENDPOINT, feature_endpoint: FEATURE_ENDPOINT, traffic_enabled: !!TRAFFIC_JOB_ID });
      });

      // Sample customers (real data from Unity Catalog via the SQL warehouse).
      app.get('/api/customers', async (req, res) => {
        try {
          const token = req.header('x-forwarded-access-token') ?? (await getToken());
          const rows = await queryRows(
            `SELECT customer_id, loyalty_tier, risk_band, annual_income, tenure_months FROM ${FQ}.customers LIMIT 50`,
            token,
          );
          res.json({ customers: rows });
        } catch (e) {
          res.status(500).json({ error: String(e) });
        }
      });

      // The four sign-in accounts: real customer_ids picked to match each archetype.
      app.get('/api/accounts', async (req, res) => {
        try {
          if (!accountsCache) {
            const token = req.header('x-forwarded-access-token') ?? (await getToken());
            const sql = ARCHETYPES.map((a) => `(SELECT '${a.key}' AS key, customer_id, loyalty_tier, risk_band, annual_income, tenure_months
              FROM ${FQ}.customers WHERE loyalty_tier = '${a.tier}' AND risk_band = '${a.risk}'
              ORDER BY abs(annual_income - ${a.income}) / ${a.income} + abs(tenure_months - ${a.tenure}) / 120.0, customer_id LIMIT 1)`).join(' UNION ALL ');
            const rows = await queryRows(sql, token);
            accountsCache = rows.map((r) => ({ ...r, annual_income: Number(r.annual_income), tenure_months: Number(r.tenure_months) }));
          }
          res.json({ accounts: accountsCache });
        } catch (e) {
          res.status(500).json({ error: String(e) });
        }
      });

      // Full offer catalog (the candidate set the ranker scores in one shot).
      app.get('/api/offers', async (req, res) => {
        try {
          const token = req.header('x-forwarded-access-token') ?? (await getToken());
          res.json({ offers: await getOffers(token) });
        } catch (e) {
          res.status(500).json({ error: String(e) });
        }
      });

      // Rank-all: score every offer for this visitor through the route-optimized ranker. The request
      // carries the visitor id, the offer columns, and the OfferMatch answers (request-time features);
      // stored and streaming features are looked up online by the endpoint. latency_ms is measured.
      app.post('/api/recommend', async (req, res) => {
        const b = (req.body ?? {}) as { customer_id?: string | null; session_id?: string; ctx?: Ctx };
        try {
          const sqlToken = req.header('x-forwarded-access-token') ?? (await getToken());
          const { scored, latencyMs } = await rankAll(visitorId(b.customer_id, b.session_id), b.ctx ?? {}, sqlToken);
          metrics.recommendation(String(scored[0]?.product_category ?? ''));
          res.json({ offers: scored, latency_ms: latencyMs, endpoint: RANKER_ENDPOINT });
        } catch (e) {
          res.status(502).json({ error: String(e) });
        }
      });

      // The visitor's features through the Feature Serving endpoint (stored + streaming + CustomUDF).
      app.post('/api/profile', async (req, res) => {
        const b = (req.body ?? {}) as { customer_id?: string | null; session_id?: string };
        try {
          const { customer, categories, latencyMs } = await lookupProfile(visitorId(b.customer_id, b.session_id));
          metrics.profile(latencyMs);
          res.json({ customer, categories, latency_ms: latencyMs });
        } catch (e) {
          res.status(502).json({ error: String(e) });
        }
      });

      // Live numbers for the scale dashboard: only what this app served and what the stream data shows.
      app.get('/api/metrics', async (req, res) => {
        try {
          res.json(await metrics.snapshot(liveCfg(req.header('x-forwarded-access-token'))));
        } catch (e) {
          res.status(500).json({ error: String(e) });
        }
      });

      // "Simulate live traffic": start/stop the continuous Kafka producer job (notebook 08).
      app.get('/api/traffic', async (_req, res) => {
        try {
          res.json(await trafficStatus(liveCfg()));
        } catch (e) {
          res.status(502).json({ error: String(e) });
        }
      });
      app.post('/api/traffic/start', async (_req, res) => {
        try {
          res.json(await trafficStart(liveCfg()));
        } catch (e) {
          res.status(502).json({ error: String(e) });
        }
      });
      app.post('/api/traffic/stop', async (_req, res) => {
        try {
          res.json(await trafficStop(liveCfg()));
        } catch (e) {
          res.status(502).json({ error: String(e) });
        }
      });

      app.post('/api/ping/start', (_req, res) => {
        latencyTestSession += 1;
        latencyTestRunning = true;
        metrics.resetLatencyMeasurement();
        res.json({ session_id: latencyTestSession });
      });

      app.post('/api/ping/stop', (_req, res) => {
        latencyTestRunning = false;
        latencyTestSession += 1; // invalidate an in-flight sample
        res.json({ stopped: true });
      });

      // One explicit, real-customer data-plane call. No background warm-up enters metrics.
      app.post('/api/ping', async (req, res) => {
        try {
          const sqlToken = req.header('x-forwarded-access-token') ?? (await getToken());
          if (!pingCustomers.length) {
            const rows = await queryRows(
              `SELECT customer_id FROM ${FQ}.customers ORDER BY xxhash64(customer_id) LIMIT 12`, sqlToken);
            pingCustomers = rows.map((r) => String(r.customer_id));
          }
          if (!pingCustomers.length) throw new Error('No customers available for the latency test.');
          const { session_id } = (req.body ?? {}) as { session_id?: number };
          const result = await rankAll(pingCustomers[pingIndex++ % pingCustomers.length], {}, sqlToken);
          if (!latencyTestRunning || session_id !== latencyTestSession) {
            res.json({ ignored: true });
            return;
          }
          res.json(metrics.latencySample(result.latencyMs));
        } catch (e) {
          res.status(502).json({ error: String(e) });
        }
      });
      app.post('/api/chat', async (req, res) => {
        const body = (req.body ?? {}) as ChatBody;
        const message = (body.message ?? '').toString();
        if (!message.trim()) {
          res.status(400).json({ error: 'message required' });
          return;
        }
        const messages: unknown[] = [];
        for (const m of body.history ?? []) {
          if ((m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string' && m.content.trim()) {
            messages.push({ role: m.role, content: m.content });
          }
        }
        messages.push({ role: 'user', content: message });

        // Per-offer/customer context from the client (offer detail chat) is
        // appended to the system prompt so answers are scoped to what the
        // banker is looking at.
        const ctx = (body.context ?? '').toString().slice(0, 2000);
        const system = ctx ? `${SYSTEM}\n\nCURRENT CONTEXT (the offer + customer on screen):\n${ctx}` : SYSTEM;

        // OBO: run SQL as the signed-in analyst (forwarded token), so Unity
        // Catalog row/column security applies per user. Falls back to the
        // service principal token for local dev, where no header is present.
        const userToken = req.header('x-forwarded-access-token');
        let sqlToken: string;
        try {
          sqlToken = userToken ?? (await getToken());
        } catch (e) {
          res.status(500).json({ answer: `Auth error: ${String(e)}`, sql: [] });
          return;
        }

        const ran: string[] = [];
        try {
          for (let i = 0; i < 6; i += 1) {
            const r = await callModel(messages, system);
            if (!r.ok) {
              const t = await r.text();
              res.json({
                answer: `Model call failed (${r.status}). This is the model layer, not the SQL layer.\n\n\`\`\`\n${t.slice(0, 700)}\n\`\`\``,
                sql: ran,
              });
              return;
            }
            const data = (await r.json()) as { content?: ContentBlock[] };
            const content = data.content ?? [];
            messages.push({ role: 'assistant', content });
            const toolUses = content.filter((b) => b.type === 'tool_use');
            if (toolUses.length === 0) {
              const answer = content
                .filter((b) => b.type === 'text')
                .map((b) => b.text ?? '')
                .join('');
              res.json({ answer: answer || '(no answer)', sql: ran });
              return;
            }
            const results = [];
            for (const tu of toolUses) {
              const query = String(tu.input?.query ?? '');
              ran.push(query);
              results.push({ type: 'tool_result', tool_use_id: tu.id, content: await runSql(query, sqlToken) });
            }
            messages.push({ role: 'user', content: results });
          }
          res.json({ answer: 'Stopped after the maximum number of query iterations.', sql: ran });
        } catch (e) {
          res.status(500).json({ answer: `Server error: ${String(e)}`, sql: ran });
        }
      });
    });

    // Warm both route-optimized endpoints on boot. Twelve ranker calls match the benchmark protocol;
    // server-side p95 is read only after the lookup client and Lakebase connections are warm.
    // Fire-and-forget so app startup is not blocked.
    void (async () => {
      try {
        const token = await getToken();
        const customers = await queryRows(
          `SELECT customer_id FROM ${FQ}.customers ORDER BY xxhash64(customer_id) LIMIT 12`,
          token,
        );
        for (const row of customers) await rankAll(String(row.customer_id), {}, token);
        console.log('[warmup] ranker endpoint warmed with 12 requests');
      } catch (e) {
        console.warn('[warmup] ranker skipped:', String(e).slice(0, 200));
      }
      try {
        for (let i = 0; i < 3; i += 1) await lookupProfile(`__warmup_${i}__`);
        console.log('[warmup] feature endpoint warmed with 3 requests');
      } catch (e) {
        console.warn('[warmup] feature endpoint skipped:', String(e).slice(0, 200));
      }
    })();

    // Pre-warm the demo traffic path when the app starts. Serverless producer startup takes 60–90s;
    // doing it here means the stream is already hot when a presenter opens the dashboard. run-now is
    // idempotent through trafficStart (it reuses an active run), and the job self-terminates.
    if (TRAFFIC_JOB_ID) {
      void trafficEnsure(liveCfg())
        .then((s) => console.log(`[warmup] traffic simulator ${s.state} run=${s.run_id ?? 'none'}`))
        .catch((e) => console.warn('[warmup] traffic simulator skipped:', String(e).slice(0, 200)));
    }

    // Prime and continuously refresh the stream cache before the dashboard is opened. The refresh is
    // non-blocking after the first sample; /api/metrics stays fast while SQL reconciles in background.
    void metrics.snapshot(liveCfg())
      .then(() => console.log('[warmup] stream metrics cache primed'))
      .catch((e) => console.warn('[warmup] stream metrics skipped:', String(e).slice(0, 200)));
    setInterval(() => { void metrics.snapshot(liveCfg()); }, 1_000);

    // Keep the demo hot while the app is running. Endpoints scale to zero when idle and the producer
    // job is bounded; refresh both paths before a presenter arrives.
    // Traffic is operational plumbing and remains separate from recommendation metrics. Model Serving
    // endpoints stay provisioned during the demo, so no synthetic rank requests are needed here.
    setInterval(() => {
      void (async () => {
        try {
          if (TRAFFIC_JOB_ID) await trafficEnsure(liveCfg());
          console.log('[keepalive] traffic is warm');
        } catch (e) {
          console.warn('[keepalive] skipped:', String(e).slice(0, 200));
        }
      })();
    }, 30_000);
  },
}).catch(console.error);
