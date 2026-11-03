import { useState } from 'react';
import type { Category, StreamClick } from '@/lib/api';
import { CAT } from '@/lib/catalog';

// The Lakeshore Bank tab's view of Streaming Feature Views: each real click goes to Kafka, Real-Time Mode
// recomputes this visitor's features, and this card shows which features changed and how fast.

type Values = NonNullable<StreamClick['after']>;
const FEATURES: { key: keyof Values; label: (c: string) => string; window: string; perCategory: boolean }[] = [
  { key: 'cust_clicks_10m', label: () => 'Clicks, last 10 min', window: 'Rolling window · all pages', perCategory: false },
  { key: 'cust_cat_views_30d', label: (c) => `${c} views, last 30 days`, window: 'Sawtooth window · this category', perCategory: true },
  { key: 'cust_mobile_cat_views_10m', label: (c) => `Mobile ${c.toLowerCase()} views, last 10 min`, window: 'Rolling window · mobile app only', perCategory: true },
];
const n = (v: number | undefined) => v ?? 0;
const time = (ms: number) => new Date(ms).toTimeString().slice(0, 8);
const changed = (c: StreamClick) => Object.keys(c.feature_ms ?? {}).length;

export function StreamFeatures({ clicks, live, labels }: { clicks: StreamClick[]; live: boolean | null; labels: Map<string, string> }) {
  const [open, setOpen] = useState(true);
  const latest = clicks[0];
  const cat = (c: StreamClick) => CAT[c.category as Category]?.label ?? c.category;
  const what = (c: StreamClick) => labels.get(c.id) ?? `${c.event_type.replace(/_/g, ' ')} · ${cat(c)}`;

  if (!open) {
    return (
      <button className="sfv sfv-min" onClick={() => setOpen(true)}>
        <b>Streaming Feature Views</b>
        {latest?.status === 'served' && <span>last click: {latest.total_ms} ms</span>}
      </button>
    );
  }
  return (
    <div className="sfv">
      <div className="sfv-h">
        <div>
          <b>Streaming Feature Views</b>
          <small>Your clicks -&gt; Kafka -&gt; Real-Time Mode -&gt; Feature Serving</small>
        </div>
        <button aria-label="Minimize" onClick={() => setOpen(false)}>–</button>
      </div>

      {live === false && (
        <div className="sfv-empty">Live pipelines are off. Press <b>Warm up pipelines</b> on the Live stream tab, then click a product page.</div>
      )}
      {live !== false && !latest && (
        <div className="sfv-empty">Click a product page. Each click is written to Kafka, and Real-Time Mode recomputes this visitor's features.</div>
      )}

      {latest && (
        <div className="sfv-click">
          <div className="sfv-event">
            <b>{what(latest)}</b> <span>{time(latest.sent_at)}</span>
            <small>Kafka event: {latest.event_type} · {cat(latest)}</small>
          </div>
          {latest.status === 'pending' && <div className="sfv-wait">Recomputing streaming features…</div>}
          {latest.status === 'off' && <div className="sfv-wait">Not streamed: the live pipelines were off.</div>}
          {latest.status === 'missing' && <div className="sfv-wait err">Not servable after 15 s.</div>}
          {latest.status === 'error' && <div className="sfv-wait err">Failed: {latest.error}</div>}
          {latest.status === 'served' && (
            <>
              <div className="sfv-big">
                <b>{latest.total_ms} ms</b>
                <span>from your click until every updated feature was returned by Feature Serving</span>
              </div>
              <Timeline c={latest} />
              <table className="sfv-table">
                <thead>
                  <tr><th>Streaming feature</th><th>Before</th><th>After</th><th>In Lakebase at</th><th>Served at</th></tr>
                </thead>
                <tbody>
                  {FEATURES.map((f) => {
                    const before = n(latest.before?.[f.key]);
                    const after = n(latest.after?.[f.key]);
                    const moved = after !== before;
                    const ms = latest.feature_ms?.[f.key];
                    const lb = f.key === 'cust_mobile_cat_views_10m' ? undefined : latest.lakebase_feature_ms?.[f.key];
                    const mobileOnly = f.key === 'cust_mobile_cat_views_10m';
                    return (
                      <tr key={f.key} className={moved ? 'moved' : ''}>
                        <td>
                          <b>{f.label(cat(latest))}</b>
                          <small>{f.window} · <code>{f.key}</code></small>
                        </td>
                        <td>{before}</td>
                        <td>{moved ? <b>{after}</b> : after}</td>
                        {mobileOnly && ms === undefined ? (
                          <td colSpan={2}><em>not counted: website click</em></td>
                        ) : (
                          <>
                            <td>{lb !== undefined ? <b className="ms lb">+{lb} ms</b> : <em>—</em>}</td>
                            <td>{ms !== undefined ? <b className="ms">+{ms} ms</b> : <em>not within 3 s</em>}</td>
                          </>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div className="sfv-foot">
                Offers re-ranked with the new values. The breakdown follows the click count's pipeline and its steps add
                up; the table's times are ms after the click, per feature. Real-Time Mode and Lakebase times come from the pipeline's own <code>output_time</code> and
                <code>commit_time</code> in the online table; the rest are measured by this app.
              </div>
            </>
          )}
        </div>
      )}

      {clicks.length > 1 && (
        <div className="sfv-hist">
          {clicks.slice(1, 4).map((c) => (
            <div key={c.id}>
              <span>{time(c.sent_at)}</span>
              <span className="what">{what(c)}</span>
              <span>{c.status === 'served' ? `${changed(c)} features updated · ${c.total_ms} ms` : c.status === 'pending' ? 'recomputing…' : c.status === 'off' ? 'not streamed' : c.status}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// The click's path as a breakdown: how long each step took, adding up to the total. Each step ends at a
// timestamp measured after the click (Kafka ack and Feature Serving by the app, output_time and commit_time by
// the pipeline); a step whose timestamp was not captured is merged into the next one.
function Timeline({ c }: { c: StreamClick }) {
  const points: { at?: number; label: string; source: string; cls: string }[] = [
    { at: c.kafka_ms, label: 'Kafka write', source: 'app', cls: 'k' },
    { at: c.rtm_ms, label: 'Real-Time Mode read + computed', source: 'pipeline · output_time', cls: 'r' },
    { at: c.lakebase_ms, label: 'Lakebase write', source: 'pipeline · commit_time', cls: 'p' },
    // The breakdown follows one pipeline end to end: the click count, which every click moves.
    { at: c.feature_ms?.cust_clicks_10m ?? c.total_ms, label: 'Feature Serving returned it', source: 'app', cls: 's' },
  ];
  const steps: { ms: number; label: string; source: string; cls: string }[] = [];
  let prev = 0;
  let merged: string[] = [];
  for (const p of points) {
    if (p.at === undefined) {
      merged.push(p.label);
      continue;
    }
    // Clocks differ by a few ms between the app and the pipeline, so a step can come out slightly negative.
    const ms = Math.max(0, p.at - prev);
    steps.push({ ms, label: merged.length ? [...merged, p.label].join(' + ') : p.label, source: p.source, cls: p.cls });
    prev = Math.max(prev, p.at);
    merged = [];
  }
  const sum = steps.reduce((a, s) => a + s.ms, 0);
  const total = Math.max(1, sum);
  return (
    <>
      <div className="sfv-sub">Breakdown for <b>Clicks, last 10 min</b>: {sum} ms</div>
      <div className="sfv-bar" aria-hidden>
        {steps.map((s) => <i key={s.cls} className={s.cls} style={{ flexGrow: Math.max(1, (s.ms / total) * 100) }} />)}
      </div>
      {c.pipeline_note && <div className="sfv-note">Pipeline timestamps not captured: {c.pipeline_note}.</div>}
      <div className="sfv-steps">
        {steps.map((s) => (
          <div key={s.cls}>
            <b>{s.ms < 1 ? '<1 ms' : `${s.ms} ms`}</b>
            <span><i className={s.cls} />{s.label}</span>
            <small>{s.source}</small>
          </div>
        ))}
      </div>
    </>
  );
}
