import type { ReactNode } from 'react';
import { AppWindow, Sparkles, ShieldCheck, Database, Layers, Zap, Waypoints, Cpu, KeyRound, Radio, ChevronRight, GitMerge } from 'lucide-react';

// NBO platform story, layered like the Databricks reference diagram:
//   Agentic Apps → ML Platform → Unity Catalog (governance) → Lakehouse Data,
// plus a data-flow band showing the Part 1 (batch) + Part 2 (streaming) paths
// converging through the feature store into training and real-time serving.

function Node({ icon, title, subtitle, live }: { icon: ReactNode; title: string; subtitle: string; live?: boolean }) {
  return (
    <div className="flex items-start gap-3">
      <div className="mt-0.5 flex size-11 shrink-0 items-center justify-center rounded-xl bg-white shadow-sm ring-1 ring-[var(--nbo-line)]">{icon}</div>
      <div className="min-w-0">
        <div className="flex items-center gap-1.5">
          <span className="text-[14px] font-semibold text-neutral-900">{title}</span>
          {live && <span className="size-1.5 rounded-full bg-[var(--nbo-red)] shadow-[0_0_0_3px_rgba(255,54,33,0.15)]" />}
        </div>
        <div className="text-[12px] leading-snug text-neutral-500">{subtitle}</div>
      </div>
    </div>
  );
}

function Layer({ tag, blurb, tint, children }: { tag: string; blurb: string; tint: string; children: ReactNode }) {
  return (
    <div className="rounded-2xl p-4 md:p-5" style={{ background: tint }}>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-[200px_1fr] md:items-center">
        <div>
          <div className="text-[14px] font-semibold text-neutral-900">{tag}</div>
          <div className="text-[12px] leading-snug text-neutral-500">{blurb}</div>
        </div>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">{children}</div>
      </div>
    </div>
  );
}

const RED = 'var(--nbo-red)';
const NAVY = 'var(--nbo-navy)';
const GREEN = '#0f9d76';

// ── Data-flow band ────────────────────────────────────────────────
function FlowNode({ icon, label, sub, tint = '#fff', accent = NAVY }: { icon: ReactNode; label: string; sub?: string; tint?: string; accent?: string }) {
  return (
    <div className="flex min-w-[112px] flex-col items-center rounded-xl border border-[var(--nbo-line)] px-3 py-2.5 text-center shadow-sm" style={{ background: tint }}>
      <span style={{ color: accent }}>{icon}</span>
      <span className="mt-1 text-[12px] font-semibold leading-tight text-neutral-800">{label}</span>
      {sub && <span className="text-[10px] leading-tight text-neutral-400">{sub}</span>}
    </div>
  );
}
const Arrow = () => <ChevronRight size={18} className="shrink-0 text-neutral-300" />;

function DataFlow() {
  return (
    <div className="rounded-2xl border border-[var(--nbo-line)] bg-white p-4 md:p-5">
      <div className="mb-3 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-wide text-neutral-400">
        <GitMerge size={14} /> Data flow — batch (Part 1) + streaming (Part 2) → train → serve
      </div>
      <div className="flex items-center gap-2 overflow-x-auto pb-1">
        {/* two sources converging */}
        <div className="flex flex-col gap-2">
          <FlowNode icon={<Database size={18} />} label="Lakehouse" sub="batch · customers, txns" accent="#d97706" />
          <FlowNode icon={<Radio size={18} />} label="Kafka / MSK" sub="in-session events" accent={RED} />
        </div>
        <div className="flex flex-col items-center text-neutral-300">
          <ChevronRight size={18} />
        </div>
        <FlowNode icon={<Layers size={18} />} label="Feature Views" sub="batch + streaming" accent={GREEN} tint="rgba(15,157,118,0.06)" />
        <Arrow />
        <FlowNode icon={<Cpu size={18} />} label="Train ranker" sub="point-in-time · MLflow" accent={GREEN} tint="rgba(15,157,118,0.06)" />
        <Arrow />
        <FlowNode icon={<Zap size={18} />} label="Online store" sub="Lakebase" accent={GREEN} tint="rgba(15,157,118,0.06)" />
        <Arrow />
        <FlowNode icon={<Waypoints size={18} />} label="Serving" sub="route-optimized" accent={GREEN} tint="rgba(15,157,118,0.06)" />
        <Arrow />
        <FlowNode icon={<AppWindow size={18} />} label="This app" sub="rank + explain" accent={RED} tint="rgba(255,54,33,0.05)" />
      </div>
      <div className="mt-2 flex flex-wrap gap-x-5 gap-y-1 text-[11px] text-neutral-400">
        <span><b className="text-neutral-600">Part 1</b> — batch features from the lakehouse → train → online serving</span>
        <span><b className="text-neutral-600">Part 2</b> — streaming events → the same feature store & endpoint, live</span>
      </div>
    </div>
  );
}

export function ArchitectureView() {
  return (
    <div className="mx-auto w-full max-w-5xl px-6 py-8">
      <div className="mb-6 text-center">
        <div className="mb-1 text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--nbo-red)]">
          Retail Banking · Next-Best-Offer
        </div>
        <h1 className="text-[26px] font-semibold tracking-tight text-neutral-900">
          Running NBO on the <span style={{ color: RED }}>Databricks Platform</span>
        </h1>
        <p className="mx-auto mt-1 max-w-2xl text-[13.5px] text-neutral-500">
          One governed path from customer signal to a live ranking — author features once, serve them online for real-time inference.
        </p>
      </div>

      {/* Data flow first — the batch + streaming story */}
      <div className="mb-3">
        <DataFlow />
      </div>

      <div className="space-y-3">
        <Layer tag="Agentic Apps" blurb="Where the recommendation gets used" tint="rgba(255,54,33,0.05)">
          <Node icon={<AppWindow size={20} style={{ color: RED }} />} title="Recommender App" subtitle="This app — pick a customer, rank the full catalog" live />
          <Node icon={<Sparkles size={20} style={{ color: RED }} />} title="AI Assistant" subtitle="In-app agent — ask why an offer ranked where it did" live />
        </Layer>

        <Layer tag="ML Platform" blurb="Feature store, model & serving — author once, serve online" tint="rgba(15,157,118,0.06)">
          <Node icon={<Layers size={20} style={{ color: GREEN }} />} title="Feature Views" subtitle="Feature store — batch attributes + streaming RollingWindow features" />
          <Node icon={<Zap size={20} style={{ color: GREEN }} />} title="Lakebase Online Store" subtitle="Sub-10ms feature reads (measured)" live />
          <Node icon={<Cpu size={20} style={{ color: GREEN }} />} title="Ranker Model" subtitle="LightGBM · point-in-time trained · registered in MLflow / UC" />
          <Node icon={<Waypoints size={20} style={{ color: GREEN }} />} title="Route-optimized Serving" subtitle="~30ms end-to-end inference (measured)" live />
        </Layer>

        <Layer tag="Unity Catalog" blurb="Governance layered over the lakehouse" tint="rgba(11,46,79,0.05)">
          <Node icon={<ShieldCheck size={20} style={{ color: NAVY }} />} title="Unity Catalog" subtitle="fins_industry_solutions.nbo_* — one governed schema + lineage" />
          <Node icon={<KeyRound size={20} style={{ color: NAVY }} />} title="Unity AI Gateway" subtitle="Governs the assistant's LLM calls — security + cost" live />
        </Layer>

        <Layer tag="Lakehouse Data" blurb="Unified, real-time foundation" tint="rgba(217,119,6,0.06)">
          <Node icon={<Database size={20} style={{ color: '#d97706' }} />} title="Lakeflow" subtitle="BRONZE → SILVER → GOLD pipelines · batch + streaming ingest" />
          <Node icon={<Layers size={20} style={{ color: '#d97706' }} />} title="Feature Tables" subtitle="customers · offers · labels · transactions" />
        </Layer>
      </div>

      <div className="mt-6 flex items-center justify-center gap-2 text-[11px] text-neutral-400">
        <span className="size-1.5 rounded-full bg-[var(--nbo-red)]" /> live path exercised by this app
      </div>
    </div>
  );
}
