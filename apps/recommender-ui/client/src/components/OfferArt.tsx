// Per-category illustrations rendered on a gradient panel. Vector art keeps the
// catalog image-forward without shipping/loading raster assets.
import type { ProductCategory } from '@/lib/nbo';
import { CATEGORY_META } from '@/lib/nbo';

function Art({ category }: { category: ProductCategory }) {
  const s = { fill: 'none', stroke: 'rgba(255,255,255,0.95)', strokeWidth: 2.4, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const };
  switch (category) {
    case 'credit_card':
      return (
        <g {...s}>
          <rect x="12" y="20" width="52" height="34" rx="5" />
          <path d="M12 30h52" strokeWidth={6} stroke="rgba(255,255,255,0.35)" />
          <path d="M20 44h14M40 44h6" />
          <circle cx="54" cy="45" r="4" fill="rgba(255,255,255,0.5)" stroke="none" />
        </g>
      );
    case 'savings':
      return (
        <g {...s}>
          <path d="M20 46c-4 0-7-3-7-8s4-11 13-11c6 0 9 2 12 5h6l5 4-3 3c1 2 1 4 1 6 0 3-2 6-6 6" />
          <circle cx="26" cy="30" r="1.6" fill="rgba(255,255,255,0.95)" stroke="none" />
          <path d="M22 47v4M40 47v4" />
          <path d="M38 20l6-4v8" />
        </g>
      );
    case 'personal_loan':
      return (
        <g {...s}>
          <circle cx="30" cy="30" r="11" />
          <path d="M30 24v12M27 27h4.5a2.5 2.5 0 010 5H27M27 32h5" strokeWidth={2} />
          <path d="M20 50h26a4 4 0 004-4" />
          <path d="M48 42l4 4-4 4" />
        </g>
      );
    case 'mortgage':
      return (
        <g {...s}>
          <path d="M16 36L38 18l22 18" />
          <path d="M22 34v18h32V34" />
          <rect x="33" y="40" width="10" height="12" />
        </g>
      );
    case 'investment':
      return (
        <g {...s}>
          <path d="M14 50h48" />
          <path d="M18 44l10-10 8 6 14-16" />
          <path d="M52 24h-8M52 24v8" />
        </g>
      );
  }
}

export function OfferArt({ category, className = '', size = 'md' }: { category: ProductCategory; className?: string; size?: 'sm' | 'md' }) {
  const meta = CATEGORY_META[category];
  const h = size === 'sm' ? 'h-16' : 'h-24';
  return (
    <div className={`relative overflow-hidden ${h} ${className}`} style={{ background: meta.gradient }}>
      <div className="absolute inset-0 opacity-40" style={{ background: 'radial-gradient(120% 100% at 85% -20%, rgba(255,255,255,0.5), transparent 55%)' }} />
      <svg viewBox="0 0 72 72" className="absolute right-2 -bottom-1 h-[115%] opacity-90" aria-hidden>
        <Art category={category} />
      </svg>
    </div>
  );
}
