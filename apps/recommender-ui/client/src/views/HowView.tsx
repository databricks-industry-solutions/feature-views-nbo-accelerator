import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useOutletContext } from 'react-router';
import type { ShellContext } from '@/App';
import { SNIPPETS, workspaceUrl } from '@/lib/code';

interface Step {
  t: string;
  b: string;
}

// The code for each step is real: lib/code.ts cuts it from the accelerator's files (see SNIPPETS).
const STEPS: Step[] = [
  { t: '1 · Batch features', b: 'Batch features read Delta tables in the Lakehouse with Sliding and Tumbling windows or a latest value, and are materialized to Lakebase, the online store. Each one is a single Feature(...) definition that both training and serving use.' },
  { t: '2 · Streaming features', b: 'Mobile-app activity and other channels land in Kafka. The Aggregation Engine (Real-Time Mode) maintains Rolling and Sawtooth windows and writes them straight to Lakebase in about 100 ms. The same stream also lands in the Lakehouse for backfills, online store compaction, and training. Clicks on this website need no stream: the site sends them with the request.' },
  { t: '3 · Train point-in-time correct', b: 'create_training_set joins every feature from the Lakehouse as of each label timestamp, so the model never sees the future. The model is logged with feature lineage and registered in Unity Catalog.' },
  { t: '4 · Serve one request', b: 'The app sends only customer_id, the offers, the visitor\'s answers, and their clicks this visit (request-time features). The route-optimized endpoint fetches batch and streaming features from Lakebase by key, computes CustomUDF features on demand, and the ranker returns P(accept) per offer.' },
  { t: '5 · Observe at scale', b: 'Everything at once: streaming and batch features flowing into one online store and one serving API. The benchmark notebook measures it at scale: freshness for every event, read from the online table\'s own commit times, and Feature Serving p50/p95/p99 under a steady request loop. The Live stream tab shows the same path live.' },
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

// Clicking a box shows the code that builds it.
const BOX_CODE: Record<BoxId, string> = {
  stream: 'stream-produce', lhdata: 'source-data', rtm: 'stream-rolling', lakebase: 'batch-materialize',
  fse: 'serve-fse', lakehouse: 'batch-define', infer: 'serve-ranker', train: 'train-set',
};

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

export function HowView() {
  const { config } = useOutletContext<ShellContext>();
  const [step, setStep] = useState(0);
  const [snipId, setSnipId] = useState(SNIPPETS[0].id);
  const [playing, setPlaying] = useState(false);
  const [flows, setFlows] = useState<Flow[]>([]);
  const archRef = useRef<HTMLDivElement>(null);
  const boxRefs = useRef<Partial<Record<BoxId, HTMLDivElement | null>>>({});

  const st = step + 1;
  const ends = new Set(EDGES.filter((e) => e.steps.includes(st)).flatMap((e) => [e.f, e.t]));
  const s = STEPS[step];
  const stepCode = SNIPPETS.filter((x) => x.step === step);
  // A step change (buttons, auto-play) falls back to the step's first excerpt; a box click picks its own.
  const code = stepCode.find((x) => x.id === snipId) ?? stepCode[0];
  const codeUrl = workspaceUrl(config, code.file);
  const showCode = (id: BoxId) => {
    const target = SNIPPETS.find((x) => x.id === BOX_CODE[id]);
    if (!target) return;
    setPlaying(false);
    setStep(target.step);
    setSnipId(target.id);
  };

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
      className={`box${x.cls ? ` ${x.cls}` : ''}${x.s.includes(st) || ends.has(x.id) ? ' hot' : ''}${BOX_CODE[x.id] === code.id ? ' sel' : ''}`}
      role="button"
      tabIndex={0}
      title="Show the code"
      onClick={() => showCode(x.id)}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') showCode(x.id);
      }}
    >
      <b>{x.b}</b>
      <span>{x.span}</span>
    </div>
  );

   return (
    <section>
      <div className="wrap">
        <div className="dash-h">
          <div>
            <h1>How it works</h1>
            <p>Click through one recommendation, end to end. Each step highlights what runs where; click any box to see the code that builds it.</p>
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
          </div>
          <div className="card">
            <h3>The code</h3>
            <div className="code-tabs">
              {stepCode.map((x) => (
                <button key={x.id} className={x.id === code.id ? 'on' : ''} onClick={() => setSnipId(x.id)}>
                  {x.label}
                </button>
              ))}
            </div>
            <div className="code-file">
              <span>
                {code.file}, line {code.line}
              </span>
              {codeUrl && (
                <a href={codeUrl} target="_blank" rel="noreferrer">
                  Open in workspace
                </a>
              )}
            </div>
            <pre className="code" dangerouslySetInnerHTML={{ __html: code.html }} />
          </div>
        </div>
      </div>
    </section>
  );
}
