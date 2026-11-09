import { useEffect, useMemo, useRef, useState } from 'react';
import { Play, Square, Radio, Zap, Gauge, Flame, LoaderCircle, CircleCheck, Power } from 'lucide-react';
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

/** Strip chart of data freshness per event (commit_time - event_time from the streaming online table). */
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
  const [error, setError] = useState(''); // poll errors, cleared by the next good poll
  const [actionError, setActionError] = useState(''); // Start/Stop errors, kept until the next click
  const [eps, setEps] = useState(100);
  const [busy, setBusy] = useState(false);
  const [pipeBusy, setPipeBusy] = useState(false);
  const [warmClickedAt, setWarmClickedAt] = useState(0);
  const points = useRef(new Map<string, { t: number; v: number }>());
  const cursor = useRef(0);
  // Bumped on every Start/Stop. A poll that was already in flight then carries the old traffic state, so drop it.
  const epoch = useRef(0);
  useEffect(() => {
    let stop = false;
    const tick = async () => {
      const sentIn = epoch.current;
      try {
        // The server returns every point served since the last cursor, so a late poll never leaves a gap.
        const d = await api.live(cursor.current);
        if (stop) return;
        if (sentIn !== epoch.current) {
          setTimeout(tick, 0);
          return;
        }
        cursor.current = d.cursor;
        for (const p of d.fresh_points ?? []) points.current.set(p.id, { t: p.t, v: p.v });
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
    setActionError('');
    try {
      const running = live?.traffic.running;
      if (!running) points.current.clear();
      epoch.current += 1;
      const traffic = running ? await api.trafficStop() : await api.trafficStart(eps, 600);
      epoch.current += 1;
      // Show the new state now instead of waiting for the next poll, so the button never flips back.
      setLive((l) => (l ? { ...l, traffic, last_run: running ? l.last_run : null } : l));
    } catch (e) {
      epoch.current += 1;
      setActionError(String(e));
    }
    setBusy(false);
  };

  // Step 1, Warm up: start the ingestion + Real-Time Mode pipelines. They stay off between demos to save cost.
  const pipeAction = async (on: boolean) => {
    setPipeBusy(true);
    setActionError('');
    try {
      if (on) setWarmClickedAt(Date.now());
      const pipelines = on ? await api.warmUp() : await api.turnOff();
      setLive((l) => (l ? { ...l, pipelines } : l));
    } catch (e) {
      setActionError(String(e));
    }
    setPipeBusy(false);
  };

  // While traffic is off, show the last run's frozen numbers (kept on the server) instead of empty tiles.
  // During warm-up show nothing old, so a fresh start never looks like the previous run.
  const warming = !!live?.traffic.running && !!live?.traffic.warming_up;
  const last = !live?.traffic.running ? live?.last_run ?? null : null;
  const s = warming ? undefined : last?.stats ?? live?.stats;
  const feed = useMemo(() => (warming ? [] : last?.feed ?? live?.feed ?? []), [live, last, warming]);
  const ago = last ? Math.max(0, Math.round((Date.now() - last.ended_at) / 60_000)) : 0;
  const lastLabel = !last ? '' : `last run at ${last.eps}/s · ended ${ago ? `${ago} min ago` : 'just now'}`;
  // Like every other tile, Feature Serving latency comes only from a run (live or the last one); before the
  // first run it stays empty instead of showing the server's startup warm test.
  const liveFeatureP95 = s?.feature_serving_p95 ?? null;
  const warmNote = `warming the pipeline · measuring in ${live?.traffic.warm_left_s ?? 0}s`;
  const runEps = live?.traffic.running ? live.traffic.eps ?? eps : last?.eps ?? eps;
  // Above 100 events/s only a fixed 100/s sample is timed; say so next to the rate.
  const sampleNote = runEps > 100 && s?.measured_per_s ? ` · ${Math.round(s.measured_per_s)}/s timed` : '';
  const featureLatencySub = warming && !last
    ? warmNote
    : liveFeatureP95 !== null
      ? `${last ? 'last run, final' : 'live last'} ${s?.feature_serving_window_s ?? 10}s · p50 ${ms(s?.feature_serving_p50)} · ${Math.round((s?.feature_serving_n ?? 0) / (s?.feature_serving_window_s ?? 10))} req/s, 1 key each`
      : `round trip · p50 — · ${live?.traffic.running ? 'waiting for the first reads' : 'traffic stopped'}`;

  const pipes = live?.pipelines;
  // Warm up covers the serving endpoints too; a failed endpoint warm call does not block Start.
  const servingWarming = live?.serving.status === 'WARMING';
  const power = live?.power;
  const pausingLakebase = power?.step === 'Pausing Lakebase';
  const powerStep = pausingLakebase ? null : power?.step ?? null; // Warm up's Lakebase and batch job steps
  const pipePhase = pipes?.phase ?? 'checking';
  const phase = pausingLakebase ? 'stopping' : pipePhase === 'ready' && servingWarming ? 'starting' : pipePhase;
  const rtm = pipes?.pipes.filter((p) => p.role === 'rtm') ?? [];
  const rtmUp = rtm.filter((p) => p.state === 'RUNNING' && p.update === 'RUNNING').length;
  // 'unknown' means the app could not read the pipelines; Start stays usable then, the server reports any real error.
  const ready = phase === 'ready' || phase === 'unknown';
  const trafficOn = !!live?.traffic.running;
  const warmSecs = warmClickedAt && Date.now() - warmClickedAt < 600_000 ? Math.round((Date.now() - warmClickedAt) / 1000) : null;
  const autoOff = pipes?.auto_off;
  const autoOffNote = autoOff ? ` They turn off on their own after ${autoOff.idle_min} min with no activity.` : '';
  const autoOffDone = autoOff?.turned_off_at
    ? `Turned off automatically at ${new Date(autoOff.turned_off_at).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })} after ${autoOff.idle_min} min with no activity. `
    : '';
  const pipeNote = {
    checking: 'Checking the live pipelines…',
    off: autoOffDone + (power?.paused
      ? 'Paused: the live pipelines, Lakebase and the serving endpoints are off to save cost. Step 1: warm up. Warming up takes about 3 minutes, or about 10 when the daily batch job has to catch up first.'
      : 'Step 1: warm up the live pipelines. Warming up takes about 3 minutes. They stay off between demos to save cost.'),
    starting: pipes?.queued
      ? 'Waiting for the pipelines to finish shutting down, then warming up. This adds a few minutes.'
      : powerStep
      ? `${powerStep}, then starting the pipelines and warming the serving endpoints${warmSecs !== null ? ` · ${warmSecs}s` : ''}`
      : rtm.length && rtmUp === rtm.length && !servingWarming
      ? `Pipelines running, waiting until test events come through in under 1 s${pipes?.probe_ms != null ? ` · last one ${(pipes.probe_ms / 1000).toFixed(1)} s` : ''}${warmSecs !== null ? ` · ${warmSecs}s` : ''}`
      : `Warming up takes about 3 minutes · starting Kafka ingestion, ${rtm.length || 4} Real-Time Mode pipelines and the serving endpoints · ${rtmUp}/${rtm.length || 4} pipelines running · endpoints ${servingWarming ? 'warming' : 'warm'}${warmSecs !== null ? ` · ${warmSecs}s` : ''}`,
    stopping: pausingLakebase
      ? 'Pipelines stopped. Pausing Lakebase; the serving endpoints scale to zero on their own.'
      : 'Shutting down takes about 5 minutes: the pipelines stop, then Lakebase pauses and the serving endpoints scale to zero. Press Warm up to start again as soon as they stop.',
    ready: trafficOn
      ? 'Live pipelines running'
      : `Step 2: start live traffic. The pipelines stay on until you press Turn off pipelines.${autoOffNote}`,
    unknown: 'Could not read the live pipelines',
  }[phase];
  const pipeError = pipes?.failed.length ? `Failed to start: ${pipes.failed.join(', ')}. Press Warm up to retry.` : pipes?.error ?? power?.error;

  return (
    <div className="nbo-live mx-auto w-full max-w-6xl px-6 py-7">
      <div className="mb-5 flex flex-col items-start gap-4">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--bank-green)]">Real-time path</div>
          <h1 className="text-[24px] font-semibold tracking-tight">Every click event, from Kafka to the next best offer</h1>
          <p className="mt-1 max-w-3xl text-[13px] leading-[1.55] text-neutral-500">
            This app writes website click events to Kafka on Amazon MSK. Real-Time Mode Feature View pipelines read them and
            update the Lakebase online store. Data freshness is measured per event as <code>commit_time - event_time</code>: from
            the app writing the click to Kafka until its streaming feature is committed in the online store.
          </p>
          <p className="mt-1 max-w-3xl text-[12px] text-neutral-400">
            Traffic is simulated: each event is a synthetic visitor click generated by this app, so every one can be timed on
            its own. Above 100 events/s, a fixed 100/s sample is timed and the rest is sent as load.
          </p>
        </div>
        <div className="flex flex-col items-start gap-1.5">
        <div className="flex items-center gap-2 whitespace-nowrap">
          {phase === 'ready' ? (
            <span className="flex items-center gap-1.5 rounded-xl border border-[var(--bank-line)] bg-white px-3 py-2 text-[13px] font-semibold text-[var(--bank-green)]">
              <CircleCheck size={14} /> Pipelines ready
            </span>
          ) : (
            <button
              onClick={() => pipeAction(true)}
              disabled={pipeBusy || !live || phase === 'starting' || phase === 'checking'}
              title="Warming up takes about 3 minutes"
              className="flex items-center gap-2 rounded-xl px-4 py-2 text-[13px] font-semibold shadow-sm disabled:opacity-60"
              style={{ background: 'var(--bank-glow)', color: 'var(--bank-navy)' }}
            >
              {phase === 'starting' || pipeBusy
                ? <><LoaderCircle size={14} className="animate-spin" /> Warming up…</>
                : <><Flame size={14} /> Warm up pipelines</>}
            </button>
          )}
          <select
            value={eps}
            onChange={(e) => setEps(Number(e.target.value))}
            disabled={live?.traffic.running}
            className="rounded-xl border border-[var(--bank-line)] bg-white px-3 py-2 text-[13px]"
          >
            {[5, 10, 25, 50, 100, 1000].map((v) => <option key={v} value={v}>{v.toLocaleString()} events/s</option>)}
          </select>
          <button
            onClick={toggle}
            disabled={busy || !live || (!trafficOn && !ready)}
            title={!trafficOn && !ready ? 'Warm up the pipelines first' : undefined}
            className="flex items-center gap-2 rounded-xl px-4 py-2 text-[13px] font-semibold shadow-sm disabled:opacity-50"
            style={trafficOn ? { background: 'var(--bank-navy)', color: 'white' } : ready
              ? { background: 'var(--bank-glow)', color: 'var(--bank-navy)' }
              : { background: 'white', color: 'var(--bank-navy)', border: '1px solid var(--bank-line)' }}
          >
            {busy
              ? live?.traffic.running ? <><Square size={14} /> Stopping…</> : <><Play size={14} /> Connecting to Kafka…</>
              : live?.traffic.running ? <><Square size={14} /> Stop traffic</> : <><Play size={14} /> Start live traffic</>}
          </button>
          {/* The only way the app stops the pipelines; it also stops any live traffic. */}
          <button
            onClick={() => pipeAction(false)}
            disabled={pipeBusy || !live || !(phase === 'ready' || phase === 'starting')}
            title="Shutting down takes about 5 minutes"
            className="flex items-center gap-2 rounded-xl border border-[var(--bank-line)] bg-white px-4 py-2 text-[13px] font-semibold text-[var(--bank-navy)] shadow-sm disabled:opacity-50"
          >
            {phase === 'stopping'
              ? <><LoaderCircle size={14} className="animate-spin" /> Turning off…</>
              : <><Power size={14} /> Turn off pipelines</>}
          </button>
        </div>
        <div className="text-left text-[11.5px] leading-[1.45] text-neutral-500">{pipeNote}</div>
        </div>
      </div>

      {(actionError || error || live?.traffic.error || s?.poll_error || pipeError) && (
        <div className="mb-4 rounded-xl border border-red-200 bg-red-50 px-4 py-2.5 text-[12.5px] text-red-700">
          {actionError || error || live?.traffic.error || s?.poll_error || pipeError}
        </div>
      )}

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Kpi icon={<Radio size={13} />} label="Events sent" value={s ? s.events_per_s.toFixed(1) : '—'} sub={warming ? warmNote : live?.traffic.running ? `per second${sampleNote} · ${live.traffic.ends_in_s}s left` : last ? `per second${sampleNote} · ${lastLabel}` : 'per second · traffic stopped'} />
        <Kpi icon={<Gauge size={13} />} label="Warm Feature Serving p95" value={ms(liveFeatureP95)} sub={featureLatencySub} />
        <Kpi icon={<Zap size={13} />} label="Data freshness p95" value={ms(s?.freshness_p95)} sub={warming && !last ? warmNote : `commit_time - event_time · p50 ${ms(s?.freshness_p50)}${last ? ` · ${lastLabel}` : ''}`} accent="var(--bank-green)" />
        <Kpi icon={<Gauge size={13} />} label="In flight" value={last ? '0' : s ? String(s.pending) : '—'} sub={warming ? warmNote : `${s?.n ?? 0} served · ${s?.missing ?? 0} missing, ${last ? 'last 60 s of last run' : 'last 60 s'}`} />
      </div>

      <div className="mt-3 rounded-2xl border border-[var(--bank-line)] bg-white p-4 shadow-sm">
        <div className="mb-1 flex items-center justify-between">
          <span className="text-[12px] font-semibold uppercase tracking-wide text-neutral-400">Data freshness per event, last 60 s</span>
          <span className="text-[11px] text-neutral-400">commit_time - event_time · Kafka + Real-Time Mode + online store write</span>
        </div>
        {last ? <LatencyChart points={last.fresh_points ?? []} end={last.ended_at} /> : <LatencyChart points={[...points.current.values()]} />}
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
              <th className="px-4 py-2 text-right font-semibold">Data freshness</th>
            </tr>
          </thead>
          <tbody className="tabular">
            {feed.flatMap((e) => e.targets.map((g) => (
              <tr key={`${e.event.event_id}|${g.feature}`} className={`border-t border-[var(--bank-line)] ${g.fresh_ms !== undefined ? 'nbo-flash' : ''}`}>
                <td className="px-4 py-1.5 text-neutral-500">{new Date(e.sent_at).toLocaleTimeString()}</td>
                <td className="px-4 py-1.5">{e.event.event_type}<span className="ml-1.5 text-[10.5px] text-neutral-400">{e.event.device}</span></td>
                <td className="px-4 py-1.5 text-neutral-600">{FEATURE_LABEL[g.feature] ?? g.feature}</td>
                <td className="px-4 py-1.5 font-mono text-[11.5px] text-neutral-500">{g.key}</td>
                <td className="px-4 py-1.5 text-right">{g.value ?? ''}</td>
                <td className="px-4 py-1.5 text-right font-semibold text-[var(--bank-green)]">
                  {g.fresh_ms !== undefined ? ms(g.fresh_ms) : Date.now() - e.sent_at > 30_000 ? <span className="text-red-600">not found</span> : '…'}
                </td>
              </tr>
            )))}
            {!feed.length && (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-neutral-400">{warming ? `Warming the pipeline. Measured events start in ${live?.traffic.warm_left_s ?? 0}s.` : ready ? 'Start live traffic to stream click events.' : 'Warm up the pipelines, then start live traffic to stream click events.'}</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
