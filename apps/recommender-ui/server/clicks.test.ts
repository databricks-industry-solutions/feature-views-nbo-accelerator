import { describe, expect, it } from 'vitest';
import { sessionClicks, startClicks, trackClick } from './clicks';
import type { ClickEvent } from './stream';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// A fake Kafka + Feature Serving: a produced click becomes servable after `lagMs`.
function fake(lagMs: number, live = true) {
  const produced: { e: ClickEvent; at: number }[] = [];
  let reads = 0;
  return {
    produced,
    reads: () => reads,
    deps: {
      produce: async (events: ClickEvent[]) => { for (const e of events) produced.push({ e, at: Date.now() }); },
      read: async (customerId: string, category: string) => {
        reads += 1;
        const seen = produced.filter((p) => p.e.customer_id === customerId && Date.now() - p.at >= lagMs);
        return {
          values: { cust_clicks_10m: 3 + seen.length, cust_cat_views_30d: 10 + seen.filter((p) => p.e.product_category === category).length },
          readAt: Date.now(),
        };
      },
      live: () => live,
      pollMs: 5,
      timeoutMs: 400,
    },
  };
}

describe('bank clicks', () => {
  it('follows a click from Kafka to Feature Serving, before and after', async () => {
    const f = fake(40);
    startClicks(f.deps);
    const c = trackClick('s1', 'cust_1', 'product_view', 'mortgage');
    expect(c.status).toBe('pending');
    await sleep(150);
    const [got] = sessionClicks('s1');
    expect(got).toMatchObject({ status: 'served', before: { cust_clicks_10m: 3, cust_cat_views_30d: 10 }, after: { cust_clicks_10m: 4, cust_cat_views_30d: 11 } });
    expect(got.servable_ms).toBeGreaterThanOrEqual(30);
    expect(f.produced[0].e).toMatchObject({ customer_id: 'cust_1', product_category: 'mortgage', device: 'web' });
  });

  it('keeps each before/after pair to its own click when clicks come fast', async () => {
    const f = fake(30);
    startClicks(f.deps);
    trackClick('s2', 'cust_2', 'page_view', 'savings');
    trackClick('s2', 'cust_2', 'page_view', 'savings');
    await sleep(250);
    const [second, first] = sessionClicks('s2');
    expect(first).toMatchObject({ status: 'served', before: { cust_clicks_10m: 3 }, after: { cust_clicks_10m: 4 } });
    expect(second).toMatchObject({ status: 'served', before: { cust_clicks_10m: 4 }, after: { cust_clicks_10m: 5 } });
  });

  it('does not write to Kafka while the live pipelines are off', () => {
    const f = fake(10, false);
    startClicks(f.deps);
    expect(trackClick('s3', 'cust_3', 'page_view', 'investment').status).toBe('off');
    expect(f.produced).toHaveLength(0);
  });

  it('reports a click that never becomes servable as missing', async () => {
    const f = fake(10_000);
    startClicks(f.deps);
    trackClick('s4', 'cust_4', 'page_view', 'credit_card');
    await sleep(550);
    expect(sessionClicks('s4')[0].status).toBe('missing');
  });
});
