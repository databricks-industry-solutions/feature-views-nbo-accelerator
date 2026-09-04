import { NavLink } from 'react-router';
import { Target, Network, ChevronRight } from 'lucide-react';

const NAV = [
  { to: '/', label: 'Recommender', icon: Target, end: true, hint: 'Rank offers · ask AI per offer' },
  { to: '/architecture', label: 'Architecture', icon: Network, end: false, hint: 'How it runs on Databricks' },
];

function DbxMark({ className = '' }: { className?: string }) {
  return (
    <svg viewBox="40 0 30 22" className={className} aria-hidden>
      <path
        d="m 62.064999,8.591 -8.631,4.859 L 44.192,8.258 43.747,8.498 v 3.77 l 9.686999,5.431 8.63,-4.84 v 1.995 l -8.63,4.86 -9.241999,-5.192 -0.445,0.24 v 0.646 l 9.686999,5.432 9.668,-5.432 v -3.769 l -0.445,-0.24 -9.223,5.173 L 44.784,11.732 V 9.736 l 8.649999,4.84 9.668,-5.43 V 5.43 l -0.482,-0.277 -9.186,5.155 -8.204999,-4.582 8.204999,-4.6 6.741,3.787 0.593,-0.332 V 4.119 L 53.433999,0 43.747,5.431 v 0.592 l 9.686999,5.432 8.63,-4.86 z"
        fill="var(--nbo-red)"
      />
    </svg>
  );
}

export function Sidebar() {
  return (
    <aside className="flex h-full w-[248px] shrink-0 flex-col border-r border-[var(--nbo-line)] bg-white/70 backdrop-blur-sm">
      {/* Brand */}
      <div className="flex items-center gap-2.5 px-4 py-4">
        <div className="flex size-9 items-center justify-center rounded-xl text-white shadow-sm" style={{ background: 'var(--nbo-navy)' }}>
          <Target size={18} strokeWidth={2.4} />
        </div>
        <div className="leading-tight">
          <div className="text-[14px] font-semibold tracking-tight">Next-Best-Offer</div>
          <div className="text-[11px] text-neutral-500">Retail Banking · Real-time</div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex flex-col gap-1 px-3 pt-2">
        {NAV.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            className={({ isActive }) =>
              [
                'group flex items-center gap-3 rounded-xl px-3 py-2.5 transition-colors',
                isActive ? 'bg-[var(--nbo-navy)] text-white shadow-sm' : 'text-neutral-700 hover:bg-black/[0.04]',
              ].join(' ')
            }
          >
            {({ isActive }) => (
              <>
                <item.icon size={18} strokeWidth={2.2} className={isActive ? 'text-white' : 'text-neutral-500'} />
                <span className="flex-1">
                  <span className="block text-[13.5px] font-medium leading-tight">{item.label}</span>
                  <span className={`block text-[11px] leading-tight ${isActive ? 'text-white/70' : 'text-neutral-400'}`}>{item.hint}</span>
                </span>
              </>
            )}
          </NavLink>
        ))}
      </nav>

      <div className="flex-1" />

      {/* Databricks Data + AI pitch box */}
      <div className="px-3 pb-4">
        <NavLink
          to="/architecture"
          className="relative block overflow-hidden rounded-2xl border border-[var(--nbo-line)] bg-white p-3.5 shadow-sm transition-shadow hover:shadow-md"
        >
          <div className="absolute -right-6 -top-8 size-24 rounded-full opacity-[0.07]" style={{ background: 'var(--nbo-red)' }} />
          <div className="mb-1.5 flex items-center gap-2">
            <DbxMark className="h-4 w-6" />
            <span className="text-[10px] font-semibold uppercase tracking-wider text-neutral-400">See how it&apos;s working</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-[13px] font-semibold tracking-tight text-neutral-900">Databricks Data + AI</span>
            <ChevronRight size={15} className="text-neutral-400" />
          </div>
        </NavLink>
        <div className="mt-3 px-1 text-[10.5px] leading-relaxed text-neutral-400">
          Feature Views · online store · route-optimized serving
        </div>
      </div>
    </aside>
  );
}
