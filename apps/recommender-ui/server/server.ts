import { createApp, analytics, server } from '@databricks/appkit';

// ---------------------------------------------------------------------------
// In-app FP&A agent. The agent loop runs HERE, in the app's own server (no
// separate agent endpoint).
//
// Model (MODEL_MODE): 'provider' = Claude on the customer's Anthropic
// subscription via the UC AI Gateway provider service
// `archer_finance.ai_gateway.ttan_claude`; 'fm' = hosted Databricks FM Claude.
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

const BLOCKED = /\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke|replace)\b/i;

async function runSql(statement: string, token: string): Promise<string> {
  const q = statement.trim().replace(/;+\s*$/, '');
  if (!/^\s*(select|with)\b/i.test(q)) return 'ERROR: only read-only SELECT queries are allowed.';
  if (BLOCKED.test(q)) return 'ERROR: statement contains a write keyword; only SELECT is allowed.';
  const auth = { Authorization: `Bearer ${token}` };
  const res = await fetch(`${HOST}/api/2.0/sql/statements/`, {
    method: 'POST',
    headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      warehouse_id: WAREHOUSE_ID,
      statement: q,
      wait_timeout: '30s',
      on_wait_timeout: 'CANCEL',
      format: 'JSON_ARRAY',
      disposition: 'INLINE',
    }),
  });
  if (!res.ok) return `ERROR running query: ${res.status} ${await res.text()}`;
  let data = (await res.json()) as SqlResp;
  let guard = 0;
  while (data.status?.state && ['PENDING', 'RUNNING'].includes(data.status.state) && guard < 20) {
    guard += 1;
    await new Promise((r) => setTimeout(r, 1000));
    const p = await fetch(`${HOST}/api/2.0/sql/statements/${data.statement_id}`, { headers: auth });
    data = (await p.json()) as SqlResp;
  }
  if (data.status?.state !== 'SUCCEEDED') {
    return `ERROR: query ${data.status?.state ?? 'FAILED'}: ${JSON.stringify(data.status?.error ?? {})}`;
  }
  const cols = (data.manifest?.schema?.columns ?? []).map((c) => c.name);
  const rows = (data.result?.data_array ?? []).slice(0, 1000);
  const out = rows.map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i]])));
  return JSON.stringify({ columns: cols, row_count: out.length, rows: out }).slice(0, 8000);
}

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
        res.json({ catalog: CATALOG, schema: SCHEMA });
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
  },
}).catch(console.error);
