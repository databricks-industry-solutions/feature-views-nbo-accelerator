import { useEffect, useRef, useState } from 'react';
import Markdown from 'react-markdown';
import type { Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { X, Sparkles, ArrowUp, Check, ShieldCheck, TrendingUp, Award, Code2 } from 'lucide-react';
import { CATEGORY_META, CATEGORY_DETAIL, fmtUSD, type Customer, type RankedOffer } from '@/lib/nbo';
import { OfferArt } from '@/components/OfferArt';

interface Msg { role: 'user' | 'assistant'; content: string; sql?: string[] }

const md: Components = {
  p: ({ children }) => <p className="my-1.5 leading-relaxed">{children}</p>,
  ul: ({ children }) => <ul className="my-1.5 list-disc pl-4 space-y-0.5">{children}</ul>,
  strong: ({ children }) => <strong className="font-semibold text-neutral-900">{children}</strong>,
  code: ({ children }) => <code className="rounded bg-neutral-100 px-1 py-0.5 text-[0.85em] font-mono">{children}</code>,
  table: ({ children }) => <div className="my-2 overflow-x-auto"><table className="w-full border-collapse text-[13px]">{children}</table></div>,
  th: ({ children }) => <th className="border-b border-neutral-200 px-2 py-1 text-left font-semibold">{children}</th>,
  td: ({ children }) => <td className="border-b border-neutral-100 px-2 py-1">{children}</td>,
};

function StatChip({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="rounded-xl border border-[var(--nbo-line)] bg-white px-3 py-2">
      <div className="flex items-center gap-1 text-[10.5px] font-medium uppercase tracking-wide text-neutral-400">{icon}{label}</div>
      <div className="mt-0.5 text-[15px] font-semibold text-neutral-900">{value}</div>
    </div>
  );
}

export function OfferDetail({ customer, offer, rank, onClose }: { customer: Customer; offer: RankedOffer; rank: number; onClose: () => void }) {
  const meta = CATEGORY_META[offer.product_category];
  const detail = CATEGORY_DETAIL[offer.product_category];
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<Msg[]>([]);
  const [loading, setLoading] = useState(false);
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages, loading]);
  // Reset the conversation when the viewed offer changes.
  useEffect(() => { setMessages([]); setInput(''); }, [offer.offer_id, customer.customer_id]);

  const contextString =
    `Customer ${customer.customer_id}: loyalty_tier=${customer.loyalty_tier}, risk_band=${customer.risk_band}, ` +
    `annual_income=${customer.annual_income}, tenure_months=${customer.tenure_months}. ` +
    `Offer ${offer.offer_id} (rank #${rank}, P(accept)=${(offer.score * 100).toFixed(1)}%): ` +
    `category=${offer.product_category}, "${offer.offer_text}", base_reward=${offer.base_reward}, tier_requirement=${offer.tier_requirement}.`;

  const SUGGESTIONS = [
    'Why is this the right offer for this customer?',
    'What would make them accept?',
    `How does this compare to other ${meta.label.toLowerCase()} offers?`,
  ];

  async function send(text: string) {
    const q = text.trim();
    if (!q || loading) return;
    const history = messages.map((m) => ({ role: m.role, content: m.content })).slice(-8);
    setMessages((p) => [...p, { role: 'user', content: q }]);
    setInput('');
    setLoading(true);
    try {
      const r = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: q, history, context: contextString }),
      });
      const data = (await r.json()) as { answer?: string; sql?: string[] };
      setMessages((p) => [...p, { role: 'assistant', content: data.answer ?? '(no answer)', sql: data.sql }]);
    } catch (e) {
      setMessages((p) => [...p, { role: 'assistant', content: `Request failed: ${String(e)}` }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 z-40">
      <div className="absolute inset-0 bg-black/30 backdrop-blur-[2px]" onClick={onClose} />
      <div className="absolute right-0 top-0 flex h-full w-full max-w-[540px] flex-col bg-[var(--nbo-canvas)] shadow-2xl nbo-fade" style={{ animationDuration: '.25s' }}>
        {/* Hero */}
        <div className="relative">
          <OfferArt category={offer.product_category} />
          <button onClick={onClose} className="absolute right-3 top-3 flex size-8 items-center justify-center rounded-full bg-black/25 text-white backdrop-blur-sm transition-colors hover:bg-black/40" aria-label="Close">
            <X size={16} />
          </button>
          <div className="absolute left-4 top-3 rounded-lg bg-black/25 px-2 py-0.5 text-[11px] font-semibold text-white backdrop-blur-sm">{meta.label}</div>
        </div>

        <div className="min-h-0 flex-1 overflow-auto px-5 py-4">
          <div className="mb-1 flex items-center gap-2 text-[11px] text-neutral-400">
            <span className="inline-flex items-center gap-1 font-semibold" style={{ color: meta.accent }}><Award size={12} /> Rank #{rank}</span>
            <span>·</span><span>{offer.offer_id}</span>
            <span>·</span><span>for {customer.customer_id}</span>
          </div>
          <h2 className="text-[19px] font-semibold leading-snug tracking-tight text-neutral-900">{offer.offer_text}</h2>
          <p className="mt-2 text-[13.5px] leading-relaxed text-neutral-600">{detail.overview}</p>

          {/* Stats */}
          <div className="mt-4 grid grid-cols-3 gap-2">
            <StatChip icon={<TrendingUp size={12} />} label="P(accept)" value={`${(offer.score * 100).toFixed(1)}%`} />
            <StatChip icon={<Award size={12} />} label="Base reward" value={offer.base_reward.toFixed(0)} />
            <StatChip icon={<ShieldCheck size={12} />} label="Requires" value={`tier ≥ ${offer.tier_requirement}`} />
          </div>

          {/* Why this ranked */}
          <div className="mt-4 rounded-2xl border border-[var(--nbo-line)] bg-white p-4">
            <div className="mb-2 text-[12px] font-semibold uppercase tracking-wide text-neutral-400">Why it ranked here</div>
            <ul className="space-y-1.5 text-[13px] text-neutral-700">
              <li className="flex gap-2"><Check size={15} className="mt-0.5 shrink-0 text-emerald-600" /><span><b>{customer.loyalty_tier}</b> tier · <b>{customer.risk_band}</b> risk · {fmtUSD(customer.annual_income)}/yr · {customer.tenure_months} mo tenure</span></li>
              <li className="flex gap-2"><Check size={15} className="mt-0.5 shrink-0 text-emerald-600" /><span>{detail.idealFor}</span></li>
              <li className="flex gap-2"><ShieldCheck size={15} className="mt-0.5 shrink-0 text-neutral-400" /><span>Tier requirement <b>{offer.tier_requirement}</b> {['Bronze','Silver','Gold','Platinum'].indexOf(customer.loyalty_tier) + 1 >= offer.tier_requirement ? 'met' : 'not met — score reflects reduced eligibility'}.</span></li>
            </ul>
          </div>

          {/* Features */}
          <div className="mt-3 rounded-2xl border border-[var(--nbo-line)] bg-white p-4">
            <div className="mb-2 text-[12px] font-semibold uppercase tracking-wide text-neutral-400">What&apos;s included</div>
            <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2">
              {detail.features.map((f) => (
                <div key={f} className="flex items-start gap-2 text-[13px] text-neutral-700"><Check size={14} className="mt-0.5 shrink-0" style={{ color: meta.accent }} />{f}</div>
              ))}
            </div>
            <p className="mt-3 text-[11px] leading-relaxed text-neutral-400">{detail.terms}</p>
          </div>

          {/* Ask AI about this offer */}
          <div className="mt-4">
            <div className="mb-2 flex items-center gap-1.5 text-[12px] font-semibold uppercase tracking-wide text-neutral-400">
              <Sparkles size={13} className="text-[var(--nbo-red)]" /> Ask about this offer
            </div>

            {messages.length === 0 ? (
              <div className="flex flex-wrap gap-2">
                {SUGGESTIONS.map((s) => (
                  <button key={s} onClick={() => send(s)} className="rounded-full border border-[var(--nbo-line)] bg-white px-3 py-1.5 text-left text-[12.5px] text-neutral-700 shadow-sm transition-colors hover:bg-neutral-50">{s}</button>
                ))}
              </div>
            ) : (
              <div className="space-y-3">
                {messages.map((m, i) =>
                  m.role === 'user' ? (
                    <div key={i} className="flex justify-end"><div className="max-w-[85%] rounded-2xl bg-white px-3.5 py-2 text-[13.5px] shadow-sm ring-1 ring-[var(--nbo-line)]">{m.content}</div></div>
                  ) : (
                    <div key={i} className="flex gap-2.5">
                      <Sparkles size={16} className="mt-1 shrink-0 text-[var(--nbo-red)]" strokeWidth={2.2} />
                      <div className="min-w-0 flex-1 text-[13.5px] text-neutral-800">
                        <Markdown remarkPlugins={[remarkGfm]} components={md}>{m.content}</Markdown>
                        {m.sql && m.sql.length > 0 && (
                          <details className="mt-1.5 text-[12px] text-neutral-500">
                            <summary className="inline-flex cursor-pointer items-center gap-1 select-none"><Code2 size={12} /> SQL ({m.sql.length})</summary>
                            <div className="mt-1.5 space-y-1.5">{m.sql.map((s, j) => <pre key={j} className="overflow-x-auto rounded-lg bg-neutral-900 p-2.5 text-[11px] text-neutral-100">{s}</pre>)}</div>
                          </details>
                        )}
                      </div>
                    </div>
                  ),
                )}
                {loading && <div className="flex items-center gap-2 text-neutral-500"><Sparkles size={15} className="animate-pulse text-[var(--nbo-red)]" /><span className="text-[13px]">Thinking…</span></div>}
                <div ref={endRef} />
              </div>
            )}
          </div>
        </div>

        {/* Composer */}
        <div className="border-t border-[var(--nbo-line)] bg-white/70 p-3 backdrop-blur-sm">
          <div className="flex items-end gap-2 rounded-2xl border border-[var(--nbo-line)] bg-white p-2 shadow-sm">
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); if (!loading && input.trim()) send(input); } }}
              rows={1}
              placeholder={`Ask about this ${meta.label.toLowerCase()} for ${customer.customer_id}…`}
              className="max-h-32 min-h-[24px] w-full resize-none bg-transparent px-2 py-1 text-[14px] outline-none placeholder:text-neutral-400"
            />
            <button onClick={() => input.trim() && !loading && send(input)} disabled={loading || !input.trim()} className="flex size-8 shrink-0 items-center justify-center rounded-full text-white transition-opacity disabled:opacity-30" style={{ background: 'var(--nbo-navy)' }} aria-label="Send">
              <ArrowUp size={16} />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
