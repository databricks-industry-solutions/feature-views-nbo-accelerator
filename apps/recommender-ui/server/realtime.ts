// ---------------------------------------------------------------------------
// Recommendation metrics for the scale view: what the Lakeshore Bank site served (count, rate, and the
// top-1 category mix) plus the feature-view table. Live traffic, serving latency, and event -> servable
// are measured in stream.ts.
// ---------------------------------------------------------------------------
import { servableP50Seconds } from './stream.js';

const WINDOW_MS = 5 * 60_000;
const recommendationEvents: { t: number }[] = [];
const top1: { t: number; cat: string }[] = [];

function prune<T extends { t: number }>(a: T[]): T[] {
  const cut = Date.now() - WINDOW_MS;
  while (a.length && a[0].t < cut) a.shift();
  return a;
}

export const metrics = {
  recommendation(topCategory?: string) {
    const t = Date.now();
    recommendationEvents.push({ t });
    if (topCategory) top1.push({ t, cat: topCategory });
  },
  snapshot() {
    prune(recommendationEvents);
    prune(top1);
    const now = Date.now();
    const span = Math.max(1, Math.min(WINDOW_MS, now - (recommendationEvents[0]?.t ?? now)) / 1000);
    const mix: Record<string, number> = {};
    top1.forEach((x) => (mix[x.cat] = (mix[x.cat] ?? 0) + 1));
    Object.keys(mix).forEach((k) => (mix[k] = mix[k] / top1.length));
    const fresh = servableP50Seconds();
    return {
      window_s: WINDOW_MS / 1000,
      recommend: { count: recommendationEvents.length, qps: recommendationEvents.length / span },
      top1_mix: mix,
      features: [
        { name: 'ctx_session_cat_views', window: 'This visit', source: 'Request', mode: 'request-time', freshness_s: null },
        { name: 'cust_mobile_cat_views_10m', window: 'Rolling 10m', source: 'Kafka', mode: 'streaming', freshness_s: null },
        { name: 'cust_clicks_10m', window: 'Rolling 10m', source: 'Kafka', mode: 'streaming', freshness_s: fresh },
        { name: 'cust_cat_views_30d', window: 'Sawtooth 30d', source: 'Kafka', mode: 'streaming', freshness_s: null },
        { name: 'cust_spend_to_income', window: 'Custom UDF', source: 'Feature views', mode: 'on demand', freshness_s: null },
        { name: 'cust_avg_balance_30d', window: 'Sliding 30d / 1d', source: 'Delta', mode: 'batch', freshness_s: null },
        { name: 'cust_spend_90d', window: 'Tumbling 90d', source: 'Delta', mode: 'batch', freshness_s: null },
        { name: 'cust_loyalty_tier', window: 'Latest value', source: 'Delta', mode: 'batch', freshness_s: null },
      ],
    };
  },
};
