import { useEffect, useMemo, useRef, useState } from 'react';
import { Play, Square, Radio, Zap, Gauge } from 'lucide-react';
import { api, type Live } from '@/lib/api';

// Same view as the card fraud console's Live view: the app writes click events to Kafka and times each
// one until the route-optimized Feature Serving endpoint returns it.

const POLL_MS = 250;
const FEATURE_LABEL: Record<string, string> = {
  cust_clicks_10m: 'Clicks, 10m',
  cust_mobile_cat_views_10m: 'Mobile category views, 10m',
};
const ms = (v: number | null | undefined) => (v === null || v === undefined ? '—' : `${Math.round(v)} ms`);

function Kpi({ icon, label, value, sub, accent }: { icon: React.ReactNode; label: string; value: string; sub?: string; accent?: string }) {
  return (
    <div className="rounded-2xl border border-[var(--bank-line)] bg-white p-4 shadow-sm">
      <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-wide text-neutral-400">{icon}{label}</div>
      <div className="tabular mt-1 text-[28px] font-semibold tracking-tight" style={{ color: accent ?? 'var(--bank-ink)' }}>{value}</div>
      {sub && <div className="text-[11.5px] text-neutral-500">{sub}</div>}
    </div>
  );
}

/** Strip chart of per-event servable latency (event_time -> returned by the Feature Serving endpoint). */
function LatencyChart({ points, end }: { points: { t: number; v: number }[]; end?: number }) {
  const W = 900, H = 150, PAD = 28;
  const now = end ?? Date.now();
  const span = 60_000;
  const max = Math.max(600, ...points.map((p) => p.v));
  const x = (t: number) => PAD + ((t - (now - span)) / span) * (W - PAD - 8);
  const y = (v: number) => H - 18 - (v / max) * (H - 34);
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-[150px] w-full">
      {[0, 200, 400, 600, 800, 1000].filter((g) => g <= max).map((g) => (
        <g key={g}>
          <line x1={PAD} x2={W - 8} y1={y(g)} y2={y(g)} stroke="rgba(11,46,79,0.08)" />
          <text x={PAD - 6} y={y(g) + 3} textAnchor="end" fontSize="9.5" fill="#999">{g}</text>
        </g>
      ))}
      {points.filter((p) => p.t > now - span).map((p, i) => (
        <circle key={i} cx={x(p.t)} cy={y(p.v)} r={2.6} fill="var(--bank-green)" opacity={0.75} />
      ))}
      <text x={W - 8} y={H - 3} textAnchor="end" fontSize="9.5" fill="#999">{end ? 'run end' : 'now'}</text>
      <text x={PAD} y={H - 3} fontSize="9.5" fill="#999">-60 s</text>
      <text x={PAD} y={10} fontSize="9.5" fill="#999">ms</text>
    </svg>
  );
}

export function DashboardView() {
  const [live, setLive] = useState<Live | null>(null);
  const [error, setError] = useState('');
  const [eps, setEps] = useState(100);
  const [busy, setBusy] = useState(false);
  const points = useRef(new Map<string, { t: number; v: number }>());
  const cursor = useRef(0);
  useEffect(() => {
    let stop = false;
    const tick = async () => {
      try {
        // The server returns every point served since the last cursor, so a late poll never leaves a gap.
        const d = await api.live(cursor.current);
        cursor.current = d.cursor;
        for (const p of d.points) points.current.set(p.id, { t: p.t, v: p.v });
        for (const [k, p] of points.current) if (p.t < Date.now() - 65_000) points.current.delete(k);
        setLive(d);
        setError('');
      } catch (e) {
        setError(String(e));
      }
      if (!stop) setTimeout(tick, POLL_MS);
    };
    tick();
    return () => { stop = true; };
  }, []);

  const toggle = async () => {
    setBusy(true);
    try {
      if (live?.traffic.running) await api.trafficStop();
      else {
        await api.trafficStart(eps, 600);
        points.current.clear();
      }
    } catch (e) {
      setError(String(e));
    }
    setBusy(false);
  };

  // While traffic is off, show the last run's frozen numbers (kept on the server) instead of empty tiles.
  // During warm-up show nothing old, so a fresh start never looks like the previous run.
  const warming = !!live?.traffic.running && !!live?.traffic.warming_up;
  const last = !live?.traffic.running ? live?.last_run ?? null : null;
  const s = warming ? undefined : last?.stats ?? live?.stats;
  const feed = useMemo(() => (warming ? [] : last?.feed ?? live?.feed ?? []), [live, last, warming]);
  const ago = last ? Math.max(0, Math.round((Date.now() - last.ended_at) / 60_000)) : 0;
  const lastLabel = !last ? '' : `last run at ${last.eps}/s · ended ${ago ? `${ago} min ago` : 'just now'}`;
  const liveFeatureP95 = s?.feature_serving_p95 ?? null;
  const baselineFeatureP95 = s?.feature_serving_baseline_p95 ?? null;
  const shownFeatureP95 = liveFeatureP95 ?? baselineFeatureP95;
  const warmNote = `warming the pipeline · measuring in ${live?.traffic.warm_left_s ?? 0}s`;
  const featureLatencySub = warming && !last
    ? warmNote
    : liveFeatureP95 !== null
      ? `${last ? 'last run, final' : 'live last'} ${s?.feature_serving_window_s ?? 10}s · p50 ${ms(s?.feature_serving_p50)} · ${s?.feature_serving_n ?? 0} calls`
      : baselineFeatureP95 !== null
        ? `startup warm test · p50 ${ms(s?.feature_serving_baseline_p50)} · 12 warm-ups + ${s?.feature_serving_baseline_n ?? 0} calls`
        : s?.feature_serving_baseline_error
          ? 'warm baseline unavailable'
          : 'running warm baseline…';

  return (
    <div className="nbo-live mx-auto w-full max-w-6xl px-6 py-7">
      <div className="mb-5 flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--bank-green)]">Real-time path</div>
          <h1 className="text-[24px] font-semibold tracking-tight">Every click event, from Kafka to the next best offer</h1>
          <p className="mt-1 max-w-3xl text-[13px] leading-[1.55] text-neutral-500">
            This app writes website click events to Kafka on Amazon MSK. Real-Time Mode Feature View pipelines read them and
            update the Lakebase online store. The app then asks the route-optimized Feature Serving endpoint, which serves the
            same online features the ranker reads, until each event appears in the served feature.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={eps}
            onChange={(e) => setEps(Number(e.target.value))}
            disabled={live?.traffic.running}
            className="rounded-xl border border-[var(--bank-line)] bg-white px-3 py-2 text-[13px]"
          >
            {[5, 10, 25, 50, 100].map((v) => <option key={v} value={v}>{v} events/s</option>)}
          </select>
          <button
            onClick={toggle}
            disabled={busy}
            className="flex items-center gap-2 rounded-xl px-4 py-2 text-[13px] font-semibold shadow-sm disabled:opacity-50"
            style={live?.traffic.running ? { background: 'var(--bank-navy)', color: 'white' } : { background: 'var(--bank-glow)', color: 'var(--bank-navy)' }}
          >
            {live?.traffic.running ? <><Square size={14} /> Stop traffic</> : <><Play size={14} /> Start live traffic</>}
          </button>
        </div>
      </div>

      {(error || live?.traffic.error || s?.poll_error) && (
        <div className="mb-4 rounded-xl border border-red-200 bg-red-50 px-4 py-2.5 text-[12.5px] text-red-700">
          {error || live?.traffic.error || s?.poll_error}
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Kpi icon={<Radio size={13} />} label="Events sent" value={s ? s.events_per_s.toFixed(1) : '—'} sub={warming ? warmNote : live?.traffic.running ? `per second · ${live.traffic.ends_in_s}s left` : last ? `per second · ${lastLabel}` : 'per second · traffic stopped'} />
        <Kpi icon={<Gauge size={13} />} label="Warm Feature Serving p95" value={ms(shownFeatureP95)} sub={featureLatencySub} />
        <Kpi icon={<Zap size={13} />} label="Event -> servable p95" value={ms(s?.served_p95)} sub={warming && !last ? warmNote : last ? `end-to-end · p50 ${ms(s?.served_p50)} · ${lastLabel}` : `end-to-end · p50 ${ms(s?.served_p50)} · ${s?.pending ?? 0} in flight · first read that returned it`} accent="var(--bank-green)" />
        <Kpi icon={<Gauge size={13} />} label="In flight" value={last ? '0' : s ? String(s.pending) : '—'} sub={warming ? warmNote : `${s?.n ?? 0} served · ${s?.missing ?? 0} missing, ${last ? 'last 60 s of last run' : 'last 60 s'}`} />
      </div>

      <div className="mt-3 rounded-2xl border border-[var(--bank-line)] bg-white p-4 shadow-sm">
        <div className="mb-1 flex items-center justify-between">
          <span className="text-[12px] font-semibold uppercase tracking-wide text-neutral-400">Event {'->'} servable, per event, last 60 s</span>
          <span className="text-[11px] text-neutral-400">Kafka + RTM + online store + Feature Serving · first Feature Serving read that returned each event</span>
        </div>
        {last ? <LatencyChart points={last.points} end={last.ended_at} /> : <LatencyChart points={[...points.current.values()]} />}
      </div>

      <div className="mt-3 overflow-hidden rounded-2xl border border-[var(--bank-line)] bg-white shadow-sm">
        <table className="w-full text-[12.5px]">
          <thead className="bg-neutral-50 text-left text-[11px] uppercase tracking-wide text-neutral-400">
            <tr>
              <th className="px-4 py-2 font-semibold">Sent</th>
              <th className="px-4 py-2 font-semibold">Event</th>
              <th className="px-4 py-2 font-semibold">Feature updated</th>
              <th className="px-4 py-2 font-semibold">Key</th>
              <th className="px-4 py-2 text-right font-semibold">Value</th>
              <th className="px-4 py-2 text-right font-semibold">Servable after</th>
            </tr>
          </thead>
          <tbody className="tabular">
            {feed.flatMap((e) => e.targets.map((g) => (
              <tr key={`${e.event.event_id}|${g.feature}`} className={`border-t border-[var(--bank-line)] ${g.served_at ? 'nbo-flash' : ''}`}>
                <td className="px-4 py-1.5 text-neutral-500">{new Date(e.sent_at).toLocaleTimeString()}</td>
                <td className="px-4 py-1.5">{e.event.event_type}<span className="ml-1.5 text-[10.5px] text-neutral-400">{e.event.device}</span></td>
                <td className="px-4 py-1.5 text-neutral-600">{FEATURE_LABEL[g.feature] ?? g.feature}</td>
                <td className="px-4 py-1.5 font-mono text-[11.5px] text-neutral-500">{g.key}</td>
                <td className="px-4 py-1.5 text-right">{g.value ?? ''}</td>
                <td className="px-4 py-1.5 text-right font-semibold text-[var(--bank-green)]">
                  {g.served_at !== undefined ? ms(g.served_at - e.event_ms) : e.status === 'missing' ? <span className="text-red-600">missing</span> : '…'}
                </td>
              </tr>
            )))}
            {!feed.length && (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-neutral-400">{warming ? `Warming the pipeline. Measured events start in ${live?.traffic.warm_left_s ?? 0}s.` : 'Start live traffic to stream click events.'}</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
