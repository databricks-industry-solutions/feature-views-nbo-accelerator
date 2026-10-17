// Typed client for the recommender-ui server API. Every number the UI shows comes from here.

export type Category = 'credit_card' | 'savings' | 'personal_loan' | 'mortgage' | 'investment';
export type AccountKey = 'maya' | 'raj' | 'elena' | 'sam';
export type Tier = 'bronze' | 'silver' | 'gold' | 'platinum';
export type Risk = 'low' | 'medium' | 'high';
export type Credit = 'excellent' | 'good' | 'fair';
export type EventType = 'page_view' | 'product_view' | 'calculator_use' | 'preference_change' | 'add_to_cart' | 'search';

export interface AppConfig {
  catalog: string;
  schema: string;
  ranker_endpoint: string;
  feature_endpoint: string;
  traffic_enabled: boolean;
}

export interface Account {
  key: AccountKey;
  customer_id: string;
  loyalty_tier: Tier;
  risk_band: Risk;
  annual_income: number;
  tenure_months: number;
}

export interface Offer {
  offer_id: string;
  product_category: Category;
  offer_text: string;
  base_reward: number;
  tier_requirement: number;
}

export interface RankedOffer extends Offer {
  score: number;
}

export interface RecommendCtx {
  goal: 'none' | Category;
  credit: Credit;
  income: number;
  card_spend: number;
  credit_monthly_spend?: number;
  savings_deposit?: number;
  // Present while the visitor is using the Personal Loan calculator. These request-time
  // facts choose a product whose amount/term capacity fits before the model orders it.
  loan_amount?: number;
  loan_term_months?: number;
  home_price?: number;
  mortgage_down_payment_pct?: number;
  investment_amount?: number;
  investment_monthly_contribution?: number;
  // Website clicks per category in the last 10 minutes of this session (request-time feature).
  session_cat_views: Partial<Record<Category, number>>;
}

export interface RecommendResponse {
  offers: RankedOffer[];
  latency_ms: number;
  endpoint: string;
}

export interface CustomerProfile {
  cust_loyalty_tier?: string;
  cust_risk_band?: string;
  cust_annual_income?: number;
  cust_tenure_months?: number;
  cust_spend_90d?: number;
  cust_avg_balance_30d?: number;
  cust_spend_to_income?: number;
  cust_clicks_10m?: number;
}

export interface CategoryProfile {
  cust_mobile_cat_views_10m?: number;
  cust_cat_views_30d?: number;
}

export interface ProfileResponse {
  customer: CustomerProfile;
  categories: Partial<Record<Category, CategoryProfile>>;
  latency_ms: number;
}

// Live traffic, same contract as the card fraud console: the app writes each click event to Kafka and
// tracks it until the Feature Serving endpoint returns it.
export interface TrafficState { running: boolean; eps?: number; ends_in_s?: number; warming_up?: boolean; warm_left_s?: number; error?: string | null }
export interface TrackedTarget { feature: string; key: string; expect: number; served_at?: number; value?: number; resolution_ms?: number }
export interface Tracked {
  seq: number; source: string; sent_at: number; event_ms: number; status: 'pending' | 'served' | 'missing';
  event: {
    event_id: string; customer_id: string; event_time: string; event_type: string;
    product_category: string; dwell_ms: number; device: string;
  };
  targets: TrackedTarget[];
}
export interface LiveStats {
  sent: number; missing: number; events_per_s: number; pending: number; poll_error: string | null; n: number;
  served_p50: number | null; served_p95: number | null; served_p99: number | null; resolution_p50?: number | null;
  feature_serving_p50?: number | null; feature_serving_p95?: number | null; feature_serving_p99?: number | null;
  feature_serving_n?: number;
  feature_serving_window_s?: number;
  feature_serving_baseline_p50?: number | null; feature_serving_baseline_p95?: number | null;
  feature_serving_baseline_p99?: number | null; feature_serving_baseline_n?: number;
  feature_serving_baseline_at?: number | null; feature_serving_baseline_error?: string | null;
  serving_p50?: number | null; serving_p95?: number | null; serving_n?: number; serving_error?: string | null;
  produce_p50?: number | null; produce_p95?: number | null; produce_max?: number | null;
}
export interface LastRun {
  started_at: number; ended_at: number; eps: number;
  stats: LiveStats; feed: Tracked[]; points: { id: string; t: number; v: number }[];
}
export interface Live {
  server_ms: number; cursor: number; stats: LiveStats; traffic: TrafficState;
  feed: Tracked[]; // newest 40 finished events, newest first
  points: { id: string; t: number; v: number }[]; // every target served since the request's cursor
  last_run: LastRun | null; // frozen numbers of the last finished run, while traffic is off
}

export interface FeatureRow {
  name: string;
  window: string;
  source: string;
  mode: string;
  freshness_s: number | null;
}

export interface Metrics {
  window_s: number;
  recommend: { count: number; qps: number };
  top1_mix: Partial<Record<Category, number>>;
  features: FeatureRow[];
}

// One session id per page load. The server derives the guest id from it when customer_id is null.
export const SESSION_ID: string =
  typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `s-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

async function call<T>(path: string, body?: unknown, method?: string): Promise<T> {
  const res = await fetch(path, {
    method: method ?? (body === undefined ? 'GET' : 'POST'),
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  let data: unknown = null;
  try {
    data = await res.json();
  } catch {
    /* non-JSON body */
  }
  if (!res.ok) {
    const msg = (data as { error?: string } | null)?.error ?? `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return data as T;
}

export const api = {
  config: () => call<AppConfig>('/api/config'),
  accounts: () => call<{ accounts: Account[] }>('/api/accounts'),
  offers: () => call<{ offers: Offer[] }>('/api/offers'),
  recommend: (customer_id: string | null, ctx: RecommendCtx) =>
    call<RecommendResponse>('/api/recommend', { customer_id, session_id: SESSION_ID, ctx }),
  profile: (customer_id: string | null) =>
    call<ProfileResponse>('/api/profile', { customer_id, session_id: SESSION_ID }),
  traffic: () => call<TrafficState>('/api/traffic'),
  trafficStart: (eps: number, seconds: number) => call<TrafficState>('/api/traffic', { eps, seconds }),
  trafficStop: () => call<TrafficState>('/api/traffic', undefined, 'DELETE'),
  live: (cursor = 0) => call<Live>(`/api/live?cursor=${cursor}`),
  metrics: () => call<Metrics>('/api/metrics'),
};

export const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
