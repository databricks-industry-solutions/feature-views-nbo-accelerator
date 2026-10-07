import { useEffect, useState } from 'react';
import { useOutletContext } from 'react-router';
import { api, isNum, type Category, type Metrics, type TrafficState } from '@/lib/api';
import { CAT } from '@/lib/catalog';
import type { ShellContext } from '@/App';

const POLL_MS = 250;
const LATENCY_TARGET_RPS = 10;
const LATENCY_INTERVAL_MS = Math.round(1000 / LATENCY_TARGET_RPS);
const LATENCY_MAX_IN_FLIGHT = 4;
const EMPTY = 'Browse the bank site to generate traffic';

const fmtInt = (v: unknown) => (isNum(v) ? Math.round(v).toLocaleString() : '—');

// x positions by timestamp when available, else by index.
function xs(ts: number[], w: number): number[] {
  const lo = Math.min(...ts);
  const hi = Math.max(...ts);
  if (!(hi > lo)) return ts.map((_, i) => (ts.length > 1 ? (i / (ts.length - 1)) * w : w / 2));
  return ts.map((t) => ((t - lo) / (hi - lo)) * w);
}
const yOf = (v: number, h: number, max: number) => h - (Math.max(0, v) / max) * (h - 10) - 5;
const pathOf = (x: number[], y: number[]) => x.map((xi, i) => `${i ? 'L' : 'M'}${xi.toFixed(1)},${y[i].toFixed(1)}`).join(' ');
const niceMax = (v: number, stepSize: number, floor: number) => Math.max(floor, Math.ceil((v * 1.2) / stepSize) * stepSize);

function LatencyChart({ m, running }: { m: Metrics; running: boolean }) {
  const s = m.recommend?.series ?? [];
  if (s.length < 2) {
    return <div className="chart-empty">{running ? 'Measuring live latency…' : 'Start latency test to collect samples'}</div>;
  }
  const w = 600;
  const h = 180;
  const plotW = w - 75;
  const max = niceMax(Math.max(...s.map((p) => p.p95_ms ?? 0)), 50, 100);
  const requestMax = niceMax(Math.max(...s.map((p) => p.count ?? 0)), 1, 1);
  const x = xs(s.map((p) => p.t), plotW);
  const ticks = [0, 0.25, 0.5, 0.75].map((f) => Math.round(max * f));
  const requestTicks = [...new Set([0, Math.ceil(requestMax / 2), requestMax])];
  return (
    <svg className="chart" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      {ticks.map((v) => (
        <g key={v}>
          <line x1={0} x2={plotW} y1={yOf(v, h, max)} y2={yOf(v, h, max)} stroke="#eef0f3" />
          <text x={4} y={yOf(v, h, max) - 3} fontSize={9} fill="#9ca3af">
            {v}ms
          </text>
        </g>
      ))}
      {requestTicks.map((v) => (
        <text
          key={`r-${v}`}
          x={plotW + 7}
          y={Math.min(h - 8, Math.max(24, yOf(v, h, requestMax) + 3))}
          textAnchor="start"
          fontSize={9}
          fill="#94a3b8"
        >
          {v}
        </text>
      ))}
      <text x={plotW + 7} y={10} textAnchor="start" fontSize={9} fill="#94a3b8">requests/s</text>
      {max >= 300 && <line x1={0} x2={plotW} y1={yOf(300, h, max)} y2={yOf(300, h, max)} stroke="#fecaca" strokeDasharray="4 4" />}
      <path d={pathOf(x, s.map((p) => yOf(p.count ?? 0, h, requestMax)))} fill="none" stroke="#cbd5e1" strokeWidth={1.5} />
      <path d={pathOf(x, s.map((p) => yOf(p.p95_ms ?? 0, h, max)))} fill="none" stroke="#ff3621" strokeWidth={2} />
    </svg>
  );
}

function FreshChart({ m }: { m: Metrics }) {
  const f = m.freshness?.series ?? [];
  const e = m.events?.series ?? [];
  if (f.length < 2 && e.length < 2) return <div className="chart-empty">{EMPTY}</div>;
  const w = 400;
  const h = 180;
  const plotW = w - 75;
  const max = niceMax(Math.max(1, ...f.map((p) => p.s)), 1, 2);
  const eventMax = niceMax(Math.max(1, ...e.map((p) => p.count)), 25, 100);
  const allT = [...f.map((p) => p.t), ...e.map((p) => p.t)];
  const lo = Math.min(...allT);
  const hi = Math.max(...allT);
  const X = (t: number) => (hi > lo ? ((t - lo) / (hi - lo)) * plotW : plotW / 2);
  const fx = f.map((p) => X(p.t));
  const fy = f.map((p) => yOf(p.s, h, max));
  const ticks = Array.from({ length: 4 }, (_, i) => +((max * i) / 4).toFixed(1));
  const eventTicks = [...new Set([0, Math.ceil(eventMax / 2), eventMax])];
  return (
    <svg className="chart" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      {ticks.map((v) => (
        <g key={v}>
          <line x1={0} x2={plotW} y1={yOf(v, h, max)} y2={yOf(v, h, max)} stroke="#eef0f3" />
          <text x={4} y={yOf(v, h, max) - 3} fontSize={9} fill="#9ca3af">
            {v}s
          </text>
        </g>
      ))}
      {eventTicks.map((v) => (
        <text
          key={`e-${v}`}
          x={plotW + 7}
          y={Math.min(h - 8, Math.max(24, yOf(v, h, eventMax) + 3))}
          textAnchor="start"
          fontSize={9}
          fill="#d97706"
        >
          {v}
        </text>
      ))}
      <text x={plotW + 7} y={10} textAnchor="start" fontSize={9} fill="#d97706">events/s</text>
      {e.length >= 2 && (
        <path d={pathOf(e.map((p) => X(p.t)), e.map((p) => yOf(p.count, h, eventMax)))} fill="none" stroke="#fdba74" strokeWidth={1.5} />
      )}
      {f.length >= 2 && (
        <>
          <path d={`${pathOf(fx, fy)} L${fx[fx.length - 1]},${h} L${fx[0]},${h} Z`} fill="rgba(15,157,118,.08)" stroke="none" />
          <path d={pathOf(fx, fy)} fill="none" stroke="#0f9d76" strokeWidth={2} />
        </>
      )}
      {f.map((p, i) => (
        <circle key={`${p.t}-${i}`} cx={fx[i]} cy={fy[i]} r={2.5} fill="#0f9d76" />
      ))}
    </svg>
  );
}

const WCOL: Record<string, [string, string]> = {
  Rolling: ['#fff4ed', '#c2410c'],
  Sawtooth: ['#f5f3ff', '#6d28d9'],
  Custom: ['#fdf2f8', '#be185d'],
  Sliding: ['#eff6ff', '#1d4ed8'],
  Tumbling: ['#ecfeff', '#0e7490'],
  Latest: ['#f3f4f6', '#374151'],
};

export function DashboardView() {
  const { config } = useOutletContext<ShellContext>();
  const [m, setM] = useState<Metrics | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [ping, setPing] = useState<{ busy: boolean; ms: number | null; err: string | null }>({ busy: false, ms: null, err: null });
  const [latencyRunning, setLatencyRunning] = useState(false);
  const [latencyHasSample, setLatencyHasSample] = useState(false);
  const [traffic, setTraffic] = useState<TrafficState | null>(null);
  const [trafficBusy, setTrafficBusy] = useState(false);
  const [trafficErr, setTrafficErr] = useState<string | null>(null);

  // Simulator status (the notebook 08 continuous producer job). Polled so a run that ends on its own shows up.
  useEffect(() => {
    if (!config?.traffic_enabled) return;
    let alive = true;
    const load = () =>
      api.traffic().then((t) => alive && (setTraffic(t), setTrafficErr(null))).catch((e: Error) => alive && setTrafficErr(e.message));
    load();
    const id = setInterval(load, 5000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [config?.traffic_enabled]);

  const toggleTraffic = () => {
    setTrafficBusy(true);
    (traffic?.running ? api.trafficStop() : api.trafficStart())
      .then((t) => (setTraffic(t), setTrafficErr(null)))
      .catch((e: Error) => setTrafficErr(e.message))
      .finally(() => setTrafficBusy(false));
  };

  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const load = async () => {
      try {
        const d = await api.metrics();
        if (alive) {
          setM(d);
          setErr(null);
        }
      } catch (e) {
        if (alive) setErr((e as Error).message);
      } finally {
        if (alive) timer = setTimeout(() => { void load(); }, POLL_MS);
      }
    };
    void load();
    return () => {
      alive = false;
      if (timer) clearTimeout(timer);
    };
  }, []);

  useEffect(() => {
    if (!latencyRunning) return;
    let alive = true;
    let inFlight = 0;
    let sessionId: number | null = null;
    let interval: ReturnType<typeof setInterval> | null = null;
    const sample = async () => {
      if (sessionId === null) return;
      // Skip a tick instead of creating an unbounded queue if the endpoint slows down.
      if (inFlight >= LATENCY_MAX_IN_FLIGHT) return;
      inFlight += 1;
      setPing((p) => ({ ...p, busy: true, err: null }));
      try {
        const d = await api.ping(sessionId);
        if (alive && !d.ignored) {
          setLatencyHasSample(true);
          setPing({ busy: inFlight > 1, ms: isNum(d.latency_ms) ? d.latency_ms : null, err: null });
        }
      } catch (e) {
        if (alive) setPing({ busy: inFlight > 1, ms: null, err: (e as Error).message });
      } finally {
        inFlight -= 1;
        if (alive && inFlight === 0) setPing((p) => ({ ...p, busy: false }));
      }
    };
    void api.pingStart().then((d) => {
      if (!alive) return;
      sessionId = d.session_id;
      void sample();
      interval = setInterval(() => { void sample(); }, LATENCY_INTERVAL_MS);
    }).catch((e: Error) => alive && setPing({ busy: false, ms: null, err: e.message }));
    return () => {
      alive = false;
      if (interval) clearInterval(interval);
      void api.pingStop();
    };
  }, [latencyRunning]);

  const doPing = () => {
    if (latencyRunning) {
      setLatencyRunning(false);
      return;
    }
    setPing({ busy: false, ms: null, err: null });
    setLatencyHasSample(false);
    setLatencyRunning(true);
  };

  const rec = m?.recommend;
  const measuredRec = latencyHasSample ? rec : null;
  const features = m?.features ?? [];
  const streamStarting = !!traffic?.running && !(m?.events?.eps && m.events.eps > 0);
  const streamIdle = traffic?.running === false;
  const mix = Object.entries(m?.top1_mix ?? {}).filter(([, v]) => isNum(v)) as [Category, number][];
  mix.sort((a, b) => b[1] - a[1]);
  const windowS = m?.window_s;
  const freshnessAge = isNum(m?.freshness?.measured_at)
    ? Math.max(0, (Date.now() - m.freshness.measured_at) / 1000)
    : null;
  const freshnessUpdated = freshnessAge === null
    ? 'update time unavailable'
    : freshnessAge < 1
      ? 'updated now'
      : `updated ${freshnessAge.toFixed(1)}s ago`;

  return (
    <section>
      <div className="wrap">
        <div className="dash-h">
          <div>
            <h1>Real-time feature platform · live</h1>
            <p>
              {config ? <span className="mono">{`${config.catalog}.${config.schema}`}</span> : null}
              {config ? ' · ' : ''}
              {features.length ? `${features.length} feature views · ` : ''}
              Kafka -&gt; streaming Feature Views -&gt; Lakebase online store -&gt; route-optimized serving
            </p>
          </div>
          {config?.traffic_enabled && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              {traffic?.running ? (
                <span className="pill" style={{ background: '#ecfdf5', color: '#047857' }}>
                  ● live traffic ·{' '}
                  {traffic.state === 'RUNNING'
                    ? (m?.events?.eps ?? 0) > 0
                      ? 'pre-warmed and streaming'
                      : 'producer warming'
                    : traffic.state.toLowerCase()}
                </span>
              ) : (
                <span className="pill" style={{ background: '#f3f4f6', color: '#374151' }}>
                  traffic simulator off
                </span>
              )}
              <button className="btn dark" disabled={trafficBusy || !traffic} onClick={toggleTraffic}>
                {trafficBusy ? '...' : traffic?.running ? 'Stop traffic' : 'Simulate live traffic'}
              </button>
            </div>
          )}
        </div>
        {trafficErr && <div className="errbox">Traffic simulator: {trafficErr}</div>}
        {traffic?.running && traffic.state !== 'RUNNING' && (
          <div className="sub" style={{ marginBottom: 10 }}>
            Starting the producer job on serverless (about 1 minute). Visitor events start flowing into Kafka once it is RUNNING.
          </div>
        )}
        {m?.stream_error && <div className="errbox">Stream stats unavailable: {m.stream_error}</div>}

        {err && <div className="errbox">Metrics unavailable: {err}</div>}

        <div className="loadgen">
          <b>End-to-end ping</b>
          <span style={{ color: 'var(--muted)' }} className="grow">
            {latencyRunning
              ? `Running: target ${LATENCY_TARGET_RPS} real requests/sec through lookup + ranking.`
              : 'Idle: refresh does not generate latency-test requests.'}
          </span>
          {ping.err && <span style={{ color: '#be123c' }}>{ping.err}</span>}
          <b className="mono">{ping.ms != null ? `${Math.round(ping.ms)} ms` : latencyRunning ? 'Measuring…' : '—'}</b>
          <button className="btn dark" onClick={doPing}>
            {latencyRunning ? 'Stop latency test' : 'Start latency test'}
          </button>
        </div>

        <div className="kpis">
          <div className="kpi">
            <div className="l">Recommendations / sec</div>
            <div className="v">{isNum(rec?.qps) ? rec.qps.toFixed(rec.qps < 10 ? 2 : 0) : '—'}</div>
            <div className="s">rank-all, every offer per request</div>
          </div>
          <div className="kpi">
            <div className="l">Recommendations</div>
            <div className="v">{fmtInt(rec?.count)}</div>
            <div className="s">{isNum(windowS) ? `last ${windowS} s` : 'in window'}</div>
          </div>
          <div className="kpi">
            <div className="l">End-to-end p95</div>
            <div className="v">
              {fmtInt(measuredRec?.p95_ms)}
              {isNum(measuredRec?.p95_ms) && <small>ms</small>}
            </div>
            <div className="s">
              {isNum(measuredRec?.p95_ms)
                ? `${fmtInt(measuredRec.latency_samples)} measured calls`
                : 'Not measured · start latency test'}
            </div>
          </div>
          <div className="kpi">
            <div className="l">Latest request</div>
            <div className="v">
              {isNum(measuredRec?.latest_ms) ? fmtInt(measuredRec.latest_ms) : '—'}
              {isNum(measuredRec?.latest_ms) && <small>ms</small>}
            </div>
            <div className="s">
              {isNum(measuredRec?.p95_ms)
                ? `most recent measured call · p95 ${fmtInt(measuredRec.p95_ms)} ms · ${fmtInt(measuredRec.latency_samples)} calls`
                : 'No latency samples yet · start latency test'}
            </div>
          </div>
          <div className="kpi">
            <div className="l">Events delivered to Kafka</div>
            <div className="v">
              {streamStarting ? 'Starting…' : isNum(m?.events?.total) ? fmtInt(m.events.total) : '—'}
            </div>
            <div className="s">
              {streamStarting
                ? 'Starting the serverless producer (usually 60–90 s)'
                : streamIdle
                  ? 'Click Simulate live traffic to start'
                  : `${isNum(m?.events?.eps) ? m.events.eps.toFixed(1) : '—'} acknowledged events/sec · ${fmtInt(
                      m?.events?.count,
                    )} landed in the Lakehouse in the last 60 s`}
            </div>
          </div>
          <div className="kpi">
            <div className="l">Online-store freshness p95</div>
            <div className="v">
              {streamStarting ? 'Waiting…' : isNum(m?.freshness?.p95_s) ? fmtInt(m.freshness.p95_s * 1000) : '—'}
              {!streamStarting && isNum(m?.freshness?.p95_s) && <small>ms</small>}
            </div>
            <div className="s">
              {streamStarting
                ? 'Kafka → RTM → online store; updates when the first events arrive'
                : streamIdle
                  ? 'Start live traffic to measure source event → online-store commit'
                  : `source event → online-store commit · p50 ${fmtInt((m?.freshness?.p50_s ?? 0) * 1000)} ms · ${fmtInt(
                      m?.freshness?.samples,
                    )} events, last 60 s · ${freshnessUpdated}`}
            </div>
          </div>
        </div>

        <div className="grid2">
          <div className="card">
            <h3>Latency under load</h3>
            <div className="sub">
              {isNum(windowS) ? `Last ${windowS} s` : 'Rolling window'} · end-to-end request (app -&gt; endpoint -&gt; online store -&gt;
              model)
            </div>
            {m && latencyHasSample
              ? <LatencyChart m={m} running={latencyRunning} />
              : <div className="chart-empty">{err ? '—' : latencyRunning ? 'Measuring live latency…' : 'Start latency test to collect samples'}</div>}
            <div className="legend">
              <span>
                <i style={{ background: 'var(--red)' }} />
                p95
              </span>
              <span>
                <i style={{ background: '#cbd5e1' }} />
                requests/sec (right axis)
              </span>
            </div>
          </div>
          <div className="card">
            <h3>Freshness: source event -&gt; online-store commit</h3>
            <div className="sub">commit_time - event_time on the streaming online table · excludes Feature Serving round trip</div>
            {m ? <FreshChart m={m} /> : <div className="chart-empty">{err ? '—' : 'Loading metrics...'}</div>}
            <div className="legend">
              <span>
                <i style={{ background: 'var(--green)' }} />
                freshness (s)
              </span>
              <span>
                <i style={{ background: '#fdba74' }} />
                events/sec (right axis)
              </span>
            </div>
          </div>
        </div>

        <div className="grid2">
          <div className="card">
            <h3>Feature views</h3>
            <div className="sub">Window types, materialization mode, and live freshness</div>
            {features.length === 0 ? (
              <div className="chart-empty" style={{ height: 120 }}>
                {m ? 'No feature views reported' : 'Loading metrics...'}
              </div>
            ) : (
              <table className="ft">
                <thead>
                  <tr>
                    <th>Feature</th>
                    <th>Window</th>
                    <th>Source</th>
                    <th>Mode</th>
                    <th>Freshness</th>
                  </tr>
                </thead>
                <tbody>
                  {features.map((f) => {
                    const kind = (f.window ?? '').split(' ')[0];
                    const [bg, fg] = WCOL[kind] ?? WCOL.Latest;
                    const isNew = kind === 'Sawtooth' || kind === 'Custom';
                    const fresh = isNum(f.freshness_s)
                      ? `${f.freshness_s.toFixed(1)} s`
                      : /demand|request/i.test(f.mode ?? '')
                        ? 'per request'
                        : '—';
                    return (
                      <tr key={f.name}>
                        <td className="mono">
                          {f.name}
                          {isNew && <span className="new-tag">NEW</span>}
                        </td>
                        <td>
                          <span className="wtag" style={{ background: bg, color: fg }}>
                            {f.window}
                          </span>
                        </td>
                        <td>{f.source}</td>
                        <td>{f.mode}</td>
                        <td>{fresh}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
          <div className="card">
            <h3>Top-1 offer mix</h3>
            <div className="sub">Share of #1 category across recommendations served in the window. No single category should dominate.</div>
            {mix.length === 0 ? (
              <div className="chart-empty" style={{ height: 140 }}>
                {EMPTY}
              </div>
            ) : (
              mix.map(([k, v]) => {
                const share = v > 1 ? v / 100 : v;
                return (
                  <div key={k} className="mix-row">
                    <span>{CAT[k]?.label ?? k}</span>
                    <span className="bar" style={{ height: 10 }}>
                      <i style={{ width: `${share * 100}%`, background: CAT[k]?.solid ?? '#94a3b8' }} />
                    </span>
                    <b className="mono" style={{ textAlign: 'right' }}>
                      {(share * 100).toFixed(0)}%
                    </b>
                  </div>
                );
              })
            )}
          </div>
        </div>
      </div>
    </section>
  );
}
