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

export interface TrafficState {
  running: boolean;
  run_id: number | null;
  state: string;
  started_at: number | null;
  url: string | null;
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
  recommend: {
    count: number;
    qps: number;
    latest_ms: number | null;
    p50_ms: number | null;
    p95_ms: number | null;
    p99_ms: number | null;
    latency_samples: number;
    measured_at: number | null;
    series: { t: number; p50_ms: number; p95_ms: number; p99_ms: number; count: number }[];
  };
  events: { count: number; eps: number; total: number; measured_at: number; series: { t: number; count: number }[] };
  freshness: {
    samples: number;
    p50_s: number | null;
    p95_s: number | null;
    measured_at: number | null;
    series: { t: number; s: number }[];
  };
  stream_error: string | null;
  profile_round_trip: { p50_ms: number | null };
  top1_mix: Partial<Record<Category, number>>;
  features: FeatureRow[];
}

// One session id per page load. The server derives the guest id from it when customer_id is null.
export const SESSION_ID: string =
  typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `s-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;

async function call<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
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
  trafficStart: () => call<TrafficState>('/api/traffic/start', {}),
  trafficStop: () => call<TrafficState>('/api/traffic/stop', {}),
  metrics: () => call<Metrics>('/api/metrics'),
  pingStart: () => call<{ session_id: number }>('/api/ping/start', {}),
  ping: (session_id: number) =>
    call<{ latency_ms?: number; p50_ms?: number; p95_ms?: number; p99_ms?: number; samples?: number; ignored?: boolean }>(
      '/api/ping', { session_id }),
  pingStop: () => call<{ stopped: boolean }>('/api/ping/stop', {}),
};

export const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
