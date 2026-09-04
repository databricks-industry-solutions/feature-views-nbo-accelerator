import { useEffect, useMemo, useState } from 'react';
import { Search, Zap, Gauge, Trophy, ChevronDown, Building2, ShieldCheck, Target, AlertTriangle } from 'lucide-react';
import {
  CATEGORY_META,
  fmtUSD,
  normalizeCustomer,
  normalizeRankedOffer,
  type Customer,
  type RankedOffer,
} from '@/lib/nbo';
import { OfferArt } from '@/components/OfferArt';
import { OfferDetail } from '@/components/OfferDetail';

const BUDGET_MS = 300;

const TIER_STYLE: Record<Customer['loyalty_tier'], string> = {
  Platinum: 'bg-slate-900 text-white',
  Gold: 'bg-amber-100 text-amber-800',
  Silver: 'bg-neutral-200 text-neutral-700',
  Bronze: 'bg-orange-100 text-orange-800',
};
const RISK_STYLE: Record<Customer['risk_band'], string> = {
  Low: 'text-emerald-700 bg-emerald-50',
  Medium: 'text-amber-700 bg-amber-50',
  High: 'text-rose-700 bg-rose-50',
};

function ScoreBar({ score, accent }: { score: number; accent: string }) {
  return (
    <div className="h-1.5 w-full overflow-hidden rounded-full bg-black/[0.06]">
      <div className="h-full rounded-full transition-[width] duration-500" style={{ width: `${Math.round(score * 100)}%`, background: accent }} />
    </div>
  );
}

export function RecommenderView() {
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [customer, setCustomer] = useState<Customer | null>(null);
  const [query, setQuery] = useState('');
  const [ranked, setRanked] = useState<RankedOffer[] | null>(null);
  const [latency, setLatency] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [selected, setSelected] = useState<{ offer: RankedOffer; rank: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Load real customers from Unity Catalog on mount.
  useEffect(() => {
    fetch('/api/customers')
      .then((r) => r.json())
      .then((d) => {
        if (d.error) throw new Error(d.error);
        const cs = (d.customers ?? []).map(normalizeCustomer);
        setCustomers(cs);
        if (cs.length) setCustomer(cs[0]);
      })
      .catch((e) => setError(`Could not load customers: ${String(e)}`));
  }, []);

  const filtered = useMemo(() => {
    if (!ranked) return null;
    const q = query.trim().toLowerCase();
    if (!q) return ranked;
    return ranked.filter(
      (o) => o.offer_text.toLowerCase().includes(q) || CATEGORY_META[o.product_category].label.toLowerCase().includes(q),
    );
  }, [ranked, query]);

  // Rank the full catalog via the real route-optimized endpoint; latency is the measured round trip.
  async function recommend(c: Customer) {
    setBusy(true);
    setRanked(null);
    setLatency(null);
    setError(null);
    try {
      const r = await fetch('/api/recommend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customer_id: c.customer_id }),
      });
      const d = (await r.json()) as { offers?: unknown[]; latency_ms?: number; error?: string };
      if (!r.ok || d.error) throw new Error(d.error ?? `HTTP ${r.status}`);
      setRanked((d.offers ?? []).map(normalizeRankedOffer));
      setLatency(d.latency_ms ?? null);
    } catch (e) {
      setError(`Recommendation failed: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-5xl px-6 py-8">
      {/* Header */}
      <div className="mb-6">
        <div className="mb-1 inline-flex items-center gap-2 rounded-full bg-white px-3 py-1 text-[11px] font-medium text-neutral-500 shadow-sm ring-1 ring-[var(--nbo-line)]">
          <Zap size={12} className="text-[var(--nbo-red)]" /> Rank-all · online feature lookup
        </div>
        <h1 className="text-[26px] font-semibold tracking-tight text-neutral-900">Next-Best-Offer</h1>
        <p className="mt-1 text-[14px] text-neutral-500">
          Pick a customer — the ranker scores the full offer catalog from online features by <code className="rounded bg-black/[0.05] px-1 py-0.5 text-[12px]">customer_id</code>, in under {BUDGET_MS}ms.
        </p>
      </div>

      {/* Controls */}
      <div className="mb-6 grid grid-cols-1 gap-3 md:grid-cols-[1.1fr_1fr_auto]">
        {/* Customer picker */}
        <div className="relative">
          <button
            onClick={() => setPickerOpen((o) => !o)}
            className="flex w-full items-center gap-3 rounded-2xl border border-[var(--nbo-line)] bg-white px-4 py-3 text-left shadow-sm transition-shadow hover:shadow-md"
          >
            <div className="flex size-9 items-center justify-center rounded-xl bg-[var(--nbo-navy)]/[0.06] text-[var(--nbo-navy)]">
              <Building2 size={18} />
            </div>
            <div className="min-w-0 flex-1">
              <div className="truncate text-[14px] font-semibold text-neutral-900">{customer?.customer_id ?? (customers.length ? 'Select a customer' : 'Loading customers…')}</div>
              <div className="flex items-center gap-1.5 text-[11px] text-neutral-500">
                {customer && <span className={`rounded px-1.5 py-0.5 font-medium ${TIER_STYLE[customer.loyalty_tier]}`}>{customer.loyalty_tier}</span>}
                {customer && <span className={`rounded px-1.5 py-0.5 font-medium ${RISK_STYLE[customer.risk_band]}`}>{customer.risk_band} risk</span>}
              </div>
            </div>
            <ChevronDown size={16} className={`text-neutral-400 transition-transform ${pickerOpen ? 'rotate-180' : ''}`} />
          </button>
          {pickerOpen && (
            <div className="absolute z-20 mt-2 max-h-80 w-full overflow-auto rounded-2xl border border-[var(--nbo-line)] bg-white p-1.5 shadow-xl">
              {customers.map((c) => (
                <button
                  key={c.customer_id}
                  onClick={() => {
                    setCustomer(c);
                    setPickerOpen(false);
                    setRanked(null);
                    setLatency(null);
                  }}
                  className="flex w-full items-center gap-2.5 rounded-xl px-3 py-2 text-left hover:bg-black/[0.04]"
                >
                  <span className="flex-1 text-[13px] font-medium text-neutral-800">{c.customer_id}</span>
                  <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${TIER_STYLE[c.loyalty_tier]}`}>{c.loyalty_tier}</span>
                  <span className="text-[11px] text-neutral-400">{fmtUSD(c.annual_income)}/yr</span>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Search */}
        <div className="flex items-center gap-2 rounded-2xl border border-[var(--nbo-line)] bg-white px-4 shadow-sm">
          <Search size={16} className="text-neutral-400" />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter offers…"
            className="w-full bg-transparent py-3 text-[14px] text-neutral-900 outline-none placeholder:text-neutral-400"
          />
        </div>

        {/* Recommend */}
        <button
          onClick={() => customer && recommend(customer)}
          disabled={busy || !customer}
          className="inline-flex items-center justify-center gap-2 rounded-2xl px-5 py-3 text-[14px] font-semibold text-white shadow-sm transition-transform active:scale-[0.98] disabled:opacity-60"
          style={{ background: 'var(--nbo-red)' }}
        >
          <Zap size={16} /> {busy ? 'Scoring…' : 'Recommend'}
        </button>
      </div>

      {error && (
        <div className="mb-5 flex items-start gap-2 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-[13px] text-rose-700">
          <AlertTriangle size={16} className="mt-0.5 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* Latency meter */}
      {latency !== null && (
        <div className="nbo-fade mb-5 flex flex-wrap items-center gap-3">
          <div className="inline-flex items-center gap-2 rounded-xl bg-white px-3.5 py-2 shadow-sm ring-1 ring-[var(--nbo-line)]">
            <Gauge size={15} className="text-[var(--nbo-navy)]" />
            <span className="text-[13px] font-semibold text-neutral-900">{latency} ms</span>
            <span className="text-[11px] text-neutral-400">online inference · feature lookup + rank</span>
          </div>
          <div
            className={`inline-flex items-center gap-1.5 rounded-xl px-3 py-2 text-[12px] font-medium ${latency < BUDGET_MS ? 'bg-emerald-50 text-emerald-700' : 'bg-amber-50 text-amber-700'}`}
          >
            {latency < BUDGET_MS ? `✓ under ${BUDGET_MS}ms budget` : `over ${BUDGET_MS}ms — cold slot`}
          </div>
          <div className="inline-flex items-center gap-1.5 rounded-xl bg-white px-3 py-2 text-[12px] text-neutral-500 shadow-sm ring-1 ring-[var(--nbo-line)]">
            {ranked?.length ?? 0} offers scored
          </div>
        </div>
      )}

      {/* Results */}
      {filtered ? (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {filtered.map((o, i) => {
            const meta = CATEGORY_META[o.product_category];
            const trueRank = (ranked?.indexOf(o) ?? i) + 1;
            const top = trueRank === 1;
            return (
              <button
                key={o.offer_id}
                onClick={() => setSelected({ offer: o, rank: trueRank })}
                className="nbo-fade group overflow-hidden rounded-2xl border border-[var(--nbo-line)] bg-white text-left shadow-sm transition-all hover:-translate-y-0.5 hover:shadow-lg focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--nbo-navy)]"
                style={{ animationDelay: `${i * 45}ms` }}
              >
                <div className="relative">
                  <OfferArt category={o.product_category} />
                  <div className="absolute left-3 top-3 rounded-lg bg-black/25 px-2 py-0.5 text-[11px] font-semibold text-white backdrop-blur-sm">
                    {meta.label}
                  </div>
                  {top && (
                    <div className="absolute right-3 top-3 inline-flex items-center gap-1 rounded-lg bg-white px-2 py-0.5 text-[11px] font-bold text-[var(--nbo-red)] shadow">
                      <Trophy size={12} /> Top pick
                    </div>
                  )}
                  <div className="absolute -bottom-3 right-3 flex size-11 items-center justify-center rounded-full bg-white text-[13px] font-bold shadow-md ring-1 ring-black/5" style={{ color: meta.accent }}>
                    {Math.round(o.score * 100)}
                  </div>
                </div>
                <div className="p-4 pt-5">
                  <div className="mb-1 flex items-center gap-1.5 text-[11px] text-neutral-400">
                    <span className="font-mono">#{trueRank}</span>
                    <span>·</span>
                    <span>{o.offer_id}</span>
                    <span className="ml-auto text-[var(--nbo-navy)] opacity-0 transition-opacity group-hover:opacity-100">Details →</span>
                  </div>
                  <p className="mb-3 line-clamp-2 min-h-[40px] text-[13.5px] font-medium leading-snug text-neutral-800">{o.offer_text}</p>
                  <div className="mb-2 flex items-center justify-between text-[11px] text-neutral-500">
                    <span>P(accept) {(o.score * 100).toFixed(1)}%</span>
                    <span className="inline-flex items-center gap-1"><ShieldCheck size={12} /> tier ≥ {o.tier_requirement}</span>
                  </div>
                  <ScoreBar score={o.score} accent={meta.accent} />
                </div>
              </button>
            );
          })}
          {filtered.length === 0 && (
            <div className="col-span-full rounded-2xl border border-dashed border-[var(--nbo-line)] bg-white/50 py-14 text-center text-[13px] text-neutral-400">
              No offers match “{query}”.
            </div>
          )}
        </div>
      ) : (
        <div className="rounded-2xl border border-dashed border-[var(--nbo-line)] bg-white/50 py-20 text-center">
          <div className="mx-auto mb-3 flex size-12 items-center justify-center rounded-2xl bg-[var(--nbo-navy)]/[0.06] text-[var(--nbo-navy)]">
            <Target size={22} />
          </div>
          <p className="text-[14px] font-medium text-neutral-600">Pick a customer and hit Recommend</p>
          <p className="mt-1 text-[12px] text-neutral-400">The full catalog is scored in one shot — no retrieval stage.</p>
        </div>
      )}

      {selected && customer && (
        <OfferDetail customer={customer} offer={selected.offer} rank={selected.rank} onClose={() => setSelected(null)} />
      )}
    </div>
  );
}
