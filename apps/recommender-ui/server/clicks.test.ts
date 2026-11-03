import { describe, expect, it } from 'vitest';
import { sessionClicks, startClicks, trackClick } from './clicks';
import type { ClickEvent } from './stream';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// A fake Kafka + Feature Serving: a produced click becomes servable after `lagMs` (`catLagMs` for the 30-day
// category feature, which is a separate pipeline).
function fake(lagMs: number, live = true, catLagMs = lagMs) {
  const produced: { e: ClickEvent; at: number }[] = [];
  let reads = 0;
  return {
    produced,
    reads: () => reads,
    deps: {
      produce: async (events: ClickEvent[]) => { for (const e of events) produced.push({ e, at: Date.now() }); },
      reader: async () => async (customerId: string, category: string) => {
        reads += 1;
        const mine = produced.filter((p) => p.e.customer_id === customerId);
        const seen = mine.filter((p) => Date.now() - p.at >= lagMs);
        const seenCat = mine.filter((p) => p.e.product_category === category && Date.now() - p.at >= catLagMs);
        await sleep(2); // a data-plane read takes time
        return {
          values: { cust_clicks_10m: 3 + seen.length, cust_cat_views_30d: 10 + seenCat.length, cust_mobile_cat_views_10m: 0 },
          latencyMs: 2,
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
    expect(got.total_ms).toBeGreaterThanOrEqual(40);
    expect(got.kafka_ms!).toBeLessThanOrEqual(got.total_ms!);
    expect(got.rtm_ms).toBeUndefined(); // no Lakebase reader in this test
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

  it('waits for every feature the click moves and times each one', async () => {
    const f = fake(20, true, 80); // the 30-day feature's pipeline is slower
    startClicks(f.deps);
    trackClick('s5', 'cust_5', 'page_view', 'savings');
    await sleep(250);
    const [got] = sessionClicks('s5');
    expect(got).toMatchObject({ status: 'served', after: { cust_clicks_10m: 4, cust_cat_views_30d: 11 } });
    expect(got.feature_ms!.cust_clicks_10m!).toBeLessThan(got.feature_ms!.cust_cat_views_30d!);
    expect(got.feature_ms!.cust_cat_views_30d).toBe(got.total_ms);
  });

  it('adds the pipeline timestamps from the online tables', async () => {
    const f = fake(20);
    startClicks({ ...f.deps, pipeline: async (_c, _k, eventMs) => ({ clicks: { output_ms: eventMs + 12.4, commit_ms: eventMs + 25.6 }, cat30d: { commit_ms: eventMs + 31 } }) });
    trackClick('s6', 'cust_6', 'page_view', 'savings');
    await sleep(150);
    const [got] = sessionClicks('s6');
    expect(got).toMatchObject({ status: 'served', rtm_ms: 12, lakebase_ms: 26, lakebase_feature_ms: { cust_clicks_10m: 26, cust_cat_views_30d: 31 } });
  });

  it('says why the pipeline timestamps are missing', async () => {
    const f = fake(20);
    startClicks({ ...f.deps, pipeline: async () => ({ note: 'row was replaced by a window expiry before it could be read' }) });
    trackClick('s7', 'cust_7', 'page_view', 'savings');
    await sleep(150);
    const [got] = sessionClicks('s7');
    expect(got).toMatchObject({ status: 'served', pipeline_note: 'row was replaced by a window expiry before it could be read' });
    expect(got.rtm_ms).toBeUndefined();
  });
});
