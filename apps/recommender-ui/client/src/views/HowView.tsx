import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { api, isNum, type Metrics } from '@/lib/api';

interface Step {
  t: string;
  b: string;
  code: string;
  sub: string;
  src: string;
  wf?: boolean;
}

// Static architecture copy + code snippets, ported verbatim from the mock (fixed constants).
const STEPS: Step[] = [
  { t: '1 · Batch features', b: 'Batch features read Delta tables in the Lakehouse with Sliding and Tumbling windows or a latest value, and are materialized to Lakebase, the online store. Each one is a single Feature(...) definition that both training and serving use.',
    code: 'Batch window feature', sub: 'notebooks/part1_feature_views/02_define_feature_views.py',
    src: `fe.create_feature(
    source=txn_source, entity=[<span class="s">"customer_id"</span>], timeseries_column=<span class="s">"ts"</span>,
    function=AggregationFunction(operator=Avg(input=<span class="s">"balance"</span>),
        time_window=SlidingWindow(window_duration=timedelta(days=30),
                                  slide_duration=timedelta(days=1))),
    catalog_name=catalog, schema_name=schema, name=<span class="s">"cust_avg_balance_30d"</span>)` },
  { t: '2 · Streaming features', b: 'Mobile-app activity and other channels land in Kafka. The Aggregation Engine (Real-Time Mode) maintains Rolling and Sawtooth windows and writes them straight to Lakebase in about 100 ms. The same stream also lands in the Lakehouse for backfills, online store compaction, and training. Clicks on this website need no stream: the site sends them with the request.',
    code: 'Streaming features from Kafka: Rolling + NEW Sawtooth', sub: 'notebooks/part2_realtime_two_stage/07_streaming_feature_views.py',
    src: `stream_source = StreamSource(full_name=<span class="s">f"{catalog}.{schema}.session_events_stream"</span>)

<span class="c"># short window: what is this customer doing right now?</span>
fe.create_feature(source=stream_source, entity=[<span class="s">"value.customer_id"</span>],
    timeseries_column=<span class="s">"value.event_time"</span>,
    function=AggregationFunction(operator=Count(input=<span class="s">"value.event_id"</span>),
        time_window=RollingWindow(window_duration=timedelta(minutes=10))),
    catalog_name=catalog, schema_name=schema, name=<span class="s">"cust_clicks_10m"</span>)

<span class="c"># NEW (Beta): long window on a stream, fresh at the leading edge,</span>
<span class="c"># ~2 days of live state instead of 30 (rest from the ingestion table)</span>
fe.create_feature(source=stream_source, entity=[<span class="s">"value.customer_id"</span>],
    timeseries_column=<span class="s">"value.event_time"</span>,
    function=AggregationFunction(operator=Count(input=<span class="s">"value.event_id"</span>),
        time_window=SawtoothWindow(window_duration=timedelta(days=30))),
    catalog_name=catalog, schema_name=schema, name=<span class="s">"cust_product_views_30d"</span>)` },
  { t: '3 · Train point-in-time correct', b: 'create_training_set joins every feature from the Lakehouse as of each label timestamp, so the model never sees the future. The model is logged with feature lineage and registered in Unity Catalog.',
    code: 'Training set + lineage', sub: 'notebooks/part1_feature_views/04_train_ranker.py',
    src: `training_set = fe.create_training_set(
    df=labels, features=features, label=<span class="s">"accepted"</span>,
    exclude_columns=[<span class="s">"record_id"</span>, <span class="s">"ts"</span>])

fe.log_model(model=ranker, artifact_path=<span class="s">"ranker"</span>,
    flavor=mlflow.sklearn, training_set=training_set,
    registered_model_name=<span class="s">f"{catalog}.{schema}.nbo_ranker"</span>)` },
  { t: '4 · Serve one request', b: 'The app calls deployment.predict with only customer_id, the offers, the visitor\'s answers, and their clicks this visit (request-time features). The Feature Serving Endpoint fetches batch and streaming features from Lakebase by key, computes CustomUDF features on demand, and the ranker returns P(accept) per offer.',
    code: 'Request + a CustomUDF computed at serving', sub: 'POST data-plane URL, OAuth token scoped to the endpoint',
    src: `<span class="c"># NEW (Beta): CustomUDF over upstream features, computed on demand at serving</span>
fe.create_feature(
    source=FeatureViewSource(features=[spend_90d, annual_income]),
    function=CustomUDF(function_name=<span class="s">f"{catalog}.{schema}.spend_to_income"</span>,
        input_bindings={<span class="s">"spend"</span>: spend_90d.full_name, <span class="s">"income"</span>: annual_income.full_name}),
    catalog_name=catalog, schema_name=schema, name=<span class="s">"cust_spend_to_income"</span>)

<span class="c"># the app's request: no feature code, only keys and request-time inputs</span>
{ <span class="s">"dataframe_records"</span>: [
    { <span class="s">"customer_id"</span>: <span class="s">"cust_42"</span>, <span class="s">"offer_id"</span>: <span class="s">"offer_25"</span>,
      <span class="s">"ctx_goal"</span>: <span class="s">"mortgage"</span>, <span class="s">"ctx_session_cat_views"</span>: 2 },
    { <span class="s">"customer_id"</span>: <span class="s">"cust_42"</span>, <span class="s">"offer_id"</span>: <span class="s">"offer_17"</span> },
    <span class="c">... one row per offer</span>
] }
<span class="c"># features are looked up server-side: zero feature code in the app</span>`, wf: true },
  { t: '5 · Observe at scale', b: 'Everything at once: streaming and batch features flowing into one online store and one serving API. The dashboard measures freshness per event and latency per request.',
    code: 'What the dashboard reads', sub: 'GET /api/metrics in this app',
    src: `<span class="c">-- latency + QPS</span>      timings of every /api/recommend call (app -> endpoint)
<span class="c">-- freshness</span>          commit_time - event_time per event on the streaming online table
<span class="c">-- offer mix</span>          top-1 category of served recommendations
<span class="c">-- events/sec</span>         Kafka events landing in the ingestion table (last 60 s)` },
];

// Mirrors the Feature Store architecture slide.
type BoxId = 'stream' | 'lhdata' | 'rtm' | 'lakebase' | 'fse' | 'lakehouse' | 'infer' | 'train';

interface BoxDef {
  id: BoxId;
  s: number[];
  b: string;
  span: string;
  cls?: string;
}

const LEFT: BoxDef[] = [
  { id: 'stream', s: [2, 5], b: 'Stream Data', span: 'Kafka (MSK) · mobile + simulated traffic', cls: 'src' },
  { id: 'lhdata', s: [1, 5], b: 'Lakehouse Data', span: 'Delta · customers, transactions, offers', cls: 'src' },
];
const FS_ROW: BoxDef[] = [
  { id: 'rtm', s: [2, 5], b: 'Aggregation Engine (RTM)', span: 'Rolling + Sawtooth windows' },
  { id: 'lakebase', s: [1, 2, 4, 5], b: 'Lakebase', span: 'online store' },
  { id: 'fse', s: [4, 5], b: 'Feature Serving Endpoint', span: '+ CustomUDF, on demand' },
];
const LAKEHOUSE: BoxDef = {
  id: 'lakehouse',
  s: [1, 2, 3, 5],
  b: 'Lakehouse',
  span: 'Batch features, backfills, online store compaction, notebook experimentation',
  cls: 'wide',
};
const RIGHT: BoxDef[] = [
  { id: 'infer', s: [4, 5], b: 'Inference', span: 'ranker on Model Serving · Lakeshore Bank app', cls: 'dst' },
  { id: 'train', s: [3, 5], b: 'Training', span: 'point-in-time training sets', cls: 'dst' },
];

// Red = streaming features, dark = batch features. Only the current step's links are drawn.
interface EdgeDef {
  f: BoxId;
  t: BoxId;
  steps: number[];
  kind: 'stream' | 'batch';
  route?: 'up' | 'down';
  label?: string;
  both?: boolean;
  dashed?: boolean;
}
const EDGES: EdgeDef[] = [
  { f: 'lhdata', t: 'lakehouse', steps: [1, 5], kind: 'batch' },
  { f: 'lakehouse', t: 'lakebase', steps: [1, 5], kind: 'batch', route: 'up' },
  { f: 'stream', t: 'rtm', steps: [2, 5], kind: 'stream' },
  { f: 'rtm', t: 'lakebase', steps: [2, 5], kind: 'stream' },
  { f: 'stream', t: 'lakehouse', steps: [2, 5], kind: 'stream', route: 'down', dashed: true },
  { f: 'lakehouse', t: 'train', steps: [3, 5], kind: 'batch', label: 'create_training_set', both: true },
  { f: 'lakebase', t: 'fse', steps: [4, 5], kind: 'stream' },
  { f: 'fse', t: 'infer', steps: [4, 5], kind: 'stream', label: 'deployment.predict', both: true },
];

interface Flow {
  key: string;
  d: string;
  kind: 'stream' | 'batch';
  dashed?: boolean;
  both?: boolean;
  begin: string;
  label?: { text: string; x: number; y: number };
}

const ms = (v: unknown) => (isNum(v) ? `${Math.round(v)} ms` : '—');
const sec = (v: unknown) => (isNum(v) ? `${v.toFixed(1)} s` : '—');

export function HowView() {
  const [step, setStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [flows, setFlows] = useState<Flow[]>([]);
  const [m, setM] = useState<Metrics | null>(null);
  const archRef = useRef<HTMLDivElement>(null);
  const boxRefs = useRef<Partial<Record<BoxId, HTMLDivElement | null>>>({});

  const st = step + 1;
  const ends = new Set(EDGES.filter((e) => e.steps.includes(st)).flatMap((e) => [e.f, e.t]));
  const s = STEPS[step];

  // Live numbers for the "measured in this app" lines and the step-4 waterfall.
  useEffect(() => {
    let alive = true;
    const load = () =>
      api
        .metrics()
        .then((d) => alive && setM(d))
        .catch(() => undefined);
    load();
    const id = setInterval(load, 5000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  useEffect(() => {
    if (!playing) return;
    const id = setInterval(() => setStep((x) => (x + 1) % STEPS.length), 5000);
    return () => clearInterval(id);
  }, [playing]);

  const drawFlow = useCallback(() => {
    const arch = archRef.current;
    if (!arch) return;
    const A = arch.getBoundingClientRect();
    if (A.width === 0) return;
    const R = (id: BoxId) => {
      const el = boxRefs.current[id];
      const r = el ? el.getBoundingClientRect() : new DOMRect();
      return {
        l: r.left - A.left, r: r.right - A.left, t: r.top - A.top, b: r.bottom - A.top,
        cx: (r.left + r.right) / 2 - A.left, cy: (r.top + r.bottom) / 2 - A.top,
      };
    };
    const out: Flow[] = [];
    EDGES.filter((e) => e.steps.includes(step + 1)).forEach((e, i) => {
      const a = R(e.f);
      const b = R(e.t);
      let d: string;
      let label: Flow['label'];
      if (e.route === 'up') {
        // batch materialization: straight up into the online store
        d = `M${b.cx},${a.t} V${b.b}`;
      } else if (e.route === 'down') {
        // stream also lands in the Lakehouse (ingestion + compaction)
        const x = a.r + 26;
        const y = b.t + 22;
        d = `M${a.r},${a.cy + 10} H${x} V${y} H${b.l}`;
      } else {
        // same row: one straight horizontal line
        const y = (Math.max(a.t, b.t) + Math.min(a.b, b.b)) / 2;
        d = `M${a.r},${y} H${b.l}`;
        if (e.label) label = { text: e.label, x: (a.r + b.l) / 2, y: y - 8 };
      }
      out.push({ key: `${e.f}-${e.t}`, d, kind: e.kind, dashed: e.dashed, both: e.both, begin: `${(-i * 0.3).toFixed(2)}s`, label });
    });
    setFlows(out);
  }, [step]);

  useLayoutEffect(() => {
    drawFlow();
  }, [drawFlow]);

  useEffect(() => {
    const arch = archRef.current;
    if (!arch) return;
    const ro = new ResizeObserver(() => drawFlow());
    ro.observe(arch);
    window.addEventListener('resize', drawFlow);
    return () => {
      ro.disconnect();
      window.removeEventListener('resize', drawFlow);
    };
  }, [drawFlow]);

  const box = (x: BoxDef) => (
    <div
      key={x.id}
      ref={(el) => {
        boxRefs.current[x.id] = el;
      }}
      className={`box${x.cls ? ` ${x.cls}` : ''}${x.s.includes(st) || ends.has(x.id) ? ' hot' : ''}`}
    >
      <b>{x.b}</b>
      <span>{x.span}</span>
    </div>
  );

  // Step 4 compares two independently measured distributions. Do not subtract p95 server time from
  // p50 round-trip time: they are different percentiles and measurement contexts.
  const total = m?.recommend?.p50_ms;
  const serverP95 = m?.recommend?.p95_ms;
  const scale = Math.max(isNum(total) ? total : 0, isNum(serverP95) ? serverP95 : 0) * 1.15 || 1;
  const wfRows: [string, number, number | null, string][] = [
    ['Server model p95 (lookup + rank)', 0, isNum(serverP95) ? serverP95 : null, '#0f9d76'],
    ['App round trip p50', 0, isNum(total) ? total : null, '#ff3621'],
  ];

  // Platform figures from the Feature Store architecture slide, plus what this app measured live.
  const callouts: { n: string; t: string; live?: string }[] = [
    {
      n: '200 ms',
      t: 'streaming feature freshness p99, stream -> Lakebase',
      live: isNum(m?.freshness?.p95_s) ? `freshness p95 ${sec(m?.freshness?.p95_s)}` : undefined,
    },
    {
      n: '50K QPS',
      t: 'Feature Serving throughput',
      live: isNum(m?.recommend?.qps) && (m?.recommend?.count ?? 0) > 0 ? `${m!.recommend.qps.toFixed(2)} req/s from this app` : undefined,
    },
    {
      n: '40 ms',
      t: 'feature fetch p99 at that load',
      live: isNum(m?.recommend?.p95_ms) ? `actual demo request p95 ${ms(m.recommend.p95_ms)}` : undefined,
    },
    { n: '1', t: 'definition per feature: same code trains and serves' },
  ];

  return (
    <section>
      <div className="wrap">
        <div className="dash-h">
          <div>
            <h1>How it works</h1>
            <p>Click through one recommendation, end to end. Each step highlights what runs where.</p>
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            <button className="btn ghost" onClick={() => setPlaying((p) => !p)}>
              {playing ? 'Pause' : 'Auto-play'}
            </button>
            <button className="btn ghost" onClick={() => setStep((x) => (x + STEPS.length - 1) % STEPS.length)}>
              Back
            </button>
            <button className="btn primary" onClick={() => setStep((x) => (x + 1) % STEPS.length)}>
              Next step
            </button>
          </div>
        </div>
        <div className="steps">
          {STEPS.map((x, i) => (
            <button key={x.t} className={i === step ? 'on' : ''} onClick={() => setStep(i)}>
              {x.t}
            </button>
          ))}
        </div>
        <div className="arch" ref={archRef}>
          <div className="fsgrid">
            <div className="col-l">
              {LEFT.map(box)}
            </div>
            <div className="fs">
              <div className="fs-h">
                <b>Databricks Feature Store</b>
                <span>Define a feature once, use it everywhere</span>
                <code>feature = Feature(...)</code>
              </div>
              <div className="fs-row">
                {FS_ROW.map((x) => (
                  box(x)
                ))}
              </div>
              {box(LAKEHOUSE)}
            </div>
            <div className="col-r">
              {RIGHT.map((x) => (
                box(x)
              ))}
            </div>
          </div>
          <div className="legend fs-legend">
            <span>
              <i style={{ background: 'var(--red)' }} />
              Streaming features
            </span>
            <span>
              <i style={{ background: '#334155' }} />
              Batch features
            </span>
          </div>
          <svg className="flow">
            <defs>
              {(['stream', 'batch'] as const).map((k) => (
                <marker
                  key={k}
                  id={`nbo2-ah-${k}`}
                  viewBox="0 0 10 10"
                  refX={9}
                  refY={5}
                  markerWidth={7}
                  markerHeight={7}
                  orient="auto-start-reverse"
                >
                  <path d="M0,0 L10,5 L0,10 z" fill={k === 'stream' ? '#ff3621' : '#334155'} />
                </marker>
              ))}
            </defs>
            {flows.map((f) => {
              const mk = `url(#nbo2-ah-${f.kind})`;
              return (
                <g key={`${step}-${f.key}-${f.d}`}>
                  <path
                    className={`edge ${f.kind}${f.dashed ? ' dashed' : ''}`}
                    d={f.d}
                    markerEnd={mk}
                    markerStart={f.both ? mk : undefined}
                  />
                  {f.label && (
                    <text x={f.label.x} y={f.label.y} textAnchor="middle">
                      {f.label.text}
                    </text>
                  )}
                  <circle className={`dot ${f.kind}`} r={4.5}>
                    <animateMotion dur="1.8s" begin={f.begin} repeatCount="indefinite" path={f.d} />
                  </circle>
                </g>
              );
            })}
          </svg>
        </div>
        <div className="explain">
          <div className="card">
            <h3>{s.t}</h3>
            <p>{s.b}</p>
            {s.wf && (
              <div className="wf">
                <div className="sub" style={{ marginTop: 12 }}>
                  Request waterfall (live p50, measured in this app)
                </div>
                {wfRows.map(([l, s0, d, c]) => (
                  <div key={l} className="wf-row">
                    <span>{l}</span>
                    <span className="wf-track">
                      {d != null && <i style={{ left: `${(s0 / scale) * 100}%`, width: `${(d / scale) * 100}%`, background: c }} />}
                    </span>
                    <b className="mono">{ms(d)}</b>
                  </div>
                ))}
                {!isNum(total) && <div className="sub">Browse the bank site to generate traffic.</div>}
              </div>
            )}
          </div>
          <div className="card">
            <h3>{s.code}</h3>
            <div className="sub">{s.sub}</div>
            <pre className="code" dangerouslySetInnerHTML={{ __html: s.src }} />
          </div>
        </div>
        <div className="callouts">
          {callouts.map((c) => (
            <div key={c.t} className="callout">
              <div className="k">Platform capability</div>
              <div className="n">{c.n}</div>
              <div className="t">{c.t}</div>
              {c.live && (
                <div className="live-m">
                  Measured in this app: <b>{c.live}</b>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
