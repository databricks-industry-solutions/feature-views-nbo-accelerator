import { describe, expect, it } from 'vitest';
import { feedSince, lastRunSnapshot, latestFinished, pct, pointsSince, startStream, startTraffic, stats, stopTraffic, trafficState, type ClickEvent } from './stream';

const SERVABLE_AFTER_MS = 120; // simulated Kafka -> RTM -> Lakebase lag
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('stream tracker', () => {
  it('pct uses nearest rank', () => {
    expect(pct([], 95)).toBeNull();
    expect(pct([5, 1, 3, 2, 4], 50)).toBe(3);
    expect(pct(Array.from({ length: 100 }, (_, i) => i + 1), 95)).toBe(95);
  });

  it('times each produced event until Feature Serving returns it', async () => {
    const produced: { e: ClickEvent; at: number }[] = [];
    const servable = (customer: string, category: string | null) =>
      produced.filter((p) => p.e.customer_id === customer && (category === null || (p.e.product_category === category && p.e.device !== 'web')))
        .filter((p) => Date.now() - p.at >= SERVABLE_AFTER_MS).length;
    startStream({
      host: 'https://example.invalid', getToken: async () => 't', connection: 'c', credential: 'c', topic: 't',
      warmupMs: 0,
      produce: async (events) => { for (const e of events) produced.push({ e, at: Date.now() }); },
      lookup: async (rows) => ({
        outputs: rows.map((r) => ({
          cust_clicks_10m: servable(String(r.customer_id), null),
          cust_mobile_cat_views_10m: servable(String(r.customer_id), String(r.product_category)),
        })),
        latencyMs: 5,
      }),
      rank: async () => 30,
      customers: async () => ['cust_1', 'cust_2'],
    });

    await startTraffic(100, 60);
    expect(trafficState()).toMatchObject({ running: true, eps: 100 });
    await sleep(2_500);
    const s = stats();
    expect(produced.length).toBeGreaterThanOrEqual(150);
    expect(s.events_per_s).toBeGreaterThan(60);
    expect(s.missing).toBe(0);
    expect(s.n).toBeGreaterThan(100);
    expect(s.served_p50!).toBeGreaterThanOrEqual(SERVABLE_AFTER_MS - 10);
    expect(s.served_p50!).toBeLessThanOrEqual(SERVABLE_AFTER_MS + 40);
    expect(s.resolution_p50!).toBeLessThan(20); // back-to-back polling keeps the bound tight
    expect(s.feature_serving_p95).toBe(5);
    expect(s.feature_serving_baseline_p95).toBe(5);
    expect(s.feature_serving_baseline_n).toBe(40);
    expect(s.serving_p95).toBe(30);

    const feed = feedSince(0);
    expect(feed[0].event.customer_id).toMatch(/^cust_live_[0-9a-f]{12}$/);
    const mobile = feed.find((t) => t.event.device !== 'web');
    expect(mobile?.targets.map((g) => g.feature)).toEqual(['cust_clicks_10m', 'cust_mobile_cat_views_10m']);
    const web = feed.find((t) => t.event.device === 'web');
    expect(web?.targets.map((g) => g.feature)).toEqual(['cust_clicks_10m']);
    expect(feed.some((t) => t.status === 'served')).toBe(true);
    // Cursor reads return every served target exactly once, and the table only lists finished events.
    const all = pointsSince(0);
    const served = feed.flatMap((t) => t.targets).filter((g) => g.served_at !== undefined).length;
    expect(all.length).toBeGreaterThanOrEqual(served);
    expect(new Set(all.map((p) => p.id)).size).toBe(all.length);
    expect(pointsSince(Number.MAX_SAFE_INTEGER)).toHaveLength(0);
    const table = latestFinished(40);
    expect(table).toHaveLength(40);
    expect(table.every((t) => t.status !== 'pending')).toBe(true);

    stopTraffic();
    expect(trafficState().running).toBe(false);
    // The last run stays available for a page refresh after traffic stops.
    const last = lastRunSnapshot();
    expect(last?.eps).toBe(100);
    expect(last?.stats.n).toBeGreaterThan(100);
    expect(last?.feed).toHaveLength(40);
    expect(last?.points.length).toBeGreaterThan(100);
    expect(s.produce_max).toBeNull(); // fake producer: no Kafka send timings
  }, 10_000);
});
