// Display constants for the Lakeshore Bank site. Pure presentation: no scores, no model logic.
import type { Account, AccountKey, Category, Credit, CategoryProfile, CustomerProfile, Offer, RecommendCtx, Risk, Tier } from './api';

export const CATEGORIES: Category[] = ['credit_card', 'savings', 'personal_loan', 'mortgage', 'investment'];

export const CAT: Record<Category, { label: string; color: string; solid: string }> = {
  credit_card: { label: 'Credit Card', color: 'linear-gradient(135deg,#ff3621,#b4200f)', solid: '#ff3621' },
  savings: { label: 'Savings', color: 'linear-gradient(135deg,#14507e,#0b2e4f)', solid: '#14507e' },
  personal_loan: { label: 'Personal Loan', color: 'linear-gradient(135deg,#7c3aed,#4c1d95)', solid: '#7c3aed' },
  mortgage: { label: 'Mortgage', color: 'linear-gradient(135deg,#0f9d76,#065f46)', solid: '#0f9d76' },
  investment: { label: 'Investing', color: 'linear-gradient(135deg,#d97706,#92400e)', solid: '#d97706' },
};

export const NAV: { key: Category; label: string }[] = [
  { key: 'credit_card', label: 'Credit Cards' },
  { key: 'savings', label: 'Savings' },
  { key: 'personal_loan', label: 'Personal Loans' },
  { key: 'mortgage', label: 'Mortgages' },
  { key: 'investment', label: 'Investing' },
];

export const TIERS: Tier[] = ['bronze', 'silver', 'gold', 'platinum'];
export const tierLabel = (t: string | undefined | null) => (t ? t[0].toUpperCase() + t.slice(1).toLowerCase() : '');
export const tierIndex = (t: string | undefined | null) => (t ? TIERS.indexOf(t.toLowerCase() as Tier) : -1);

// tier_requirement 1..3; a customer is eligible when tier index >= tier_requirement - 1.
export const eligibilityLabel = (req: number) => (req >= 3 ? 'Gold+' : req === 2 ? 'Silver+' : 'All members');

export const CREDIT_OPTS: { v: Credit; label: string }[] = [
  { v: 'excellent', label: 'Excellent' },
  { v: 'good', label: 'Good' },
  { v: 'fair', label: 'Fair or building' },
];
export const CREDIT_FROM_RISK: Record<Risk, Credit> = { low: 'excellent', medium: 'good', high: 'fair' };

export const GOAL_OPTS: { v: Category; label: string }[] = [
  { v: 'credit_card', label: 'Earn rewards' },
  { v: 'savings', label: 'Grow my savings' },
  { v: 'personal_loan', label: 'Borrow for a big expense' },
  { v: 'mortgage', label: 'Buy or refinance a home' },
  { v: 'investment', label: 'Invest or retire' },
];
export const GOAL_TEXT: Record<Category, string> = {
  credit_card: 'earn rewards',
  savings: 'grow savings',
  personal_loan: 'borrow',
  mortgage: 'buy a home',
  investment: 'invest',
};

// Client-side persona copy for the four demo accounts. Tier, risk, income, tenure and customer_id
// always come from GET /api/accounts.
export const ACCOUNT_META: Record<AccountKey, { name: string; first: string; sub: string; color: string }> = {
  maya: { name: 'Maya Chen', first: 'Maya', sub: 'First job, building credit', color: '#c2410c' },
  raj: { name: 'Raj Patel', first: 'Raj', sub: 'Growing family, house hunting', color: '#0f9d76' },
  elena: { name: 'Elena Rossi', first: 'Elena', sub: 'Pre-retiree, planning ahead', color: '#d97706' },
  sam: { name: 'Sam Okafor', first: 'Sam', sub: 'Gig worker, variable income', color: '#7c3aed' },
};

// "Customer since 2021" style, from tenure_months.
export const tenureText = (months: number) =>
  Number.isFinite(months) ? `since ${new Date(Date.now() - months * 30.44 * 864e5).getFullYear()}` : '';

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));
export const INCOME_RANGE = { min: 25000, max: 300000, step: 5000 };
export const SPEND_RANGE = { min: 0, max: 8000, step: 250 };

export function prefillFromAccount(a: Account): Pick<RecommendCtx, 'credit' | 'income' | 'card_spend'> {
  const income = clamp(Math.round(a.annual_income / INCOME_RANGE.step) * INCOME_RANGE.step, INCOME_RANGE.min, INCOME_RANGE.max);
  const spend = clamp(Math.round((a.annual_income / 12) * 0.1 / SPEND_RANGE.step) * SPEND_RANGE.step, SPEND_RANGE.min, SPEND_RANGE.max);
  return { credit: CREDIT_FROM_RISK[a.risk_band] ?? 'good', income, card_spend: spend };
}

// Key terms per category, indexed by tier_requirement (1 starter, 2 mid, 3 premium). Mirrors offer_text copy.
const TERMS: Record<Category, [string, string][][]> = {
  credit_card: [
    [['$0', 'annual fee'], ['1%', 'cashback'], ['6 mo', 'to upgrade']],
    [['3%', 'cashback'], ['$0', 'annual fee'], ['$200', 'welcome bonus']],
    [['3x', 'miles on travel'], ['Lounge', 'access'], ['$95', 'annual fee']],
  ],
  savings: [
    [['4.5%', 'APY'], ['$0', 'minimum'], ['FDIC', 'insured']],
    [['5.1%', 'APY'], ['12 mo', 'term'], ['$500', 'minimum']],
    [['Tiered', 'rates'], ['Check', 'writing'], ['FDIC', 'insured']],
  ],
  personal_loan: [
    [['8.99%', 'APR from'], ['$15k', 'up to'], ['1 day', 'funding']],
    [['10.49%', 'APR from'], ['$0', 'origination'], ['60 mo', 'max term']],
    [['Relationship', 'rate discount'], ['$50k', 'up to'], ['1 day', 'funding']],
  ],
  mortgage: [
    [['3%', 'down'], ['First-time', 'buyer program'], ['21 days', 'to close']],
    [['6.1%', 'fixed rate'], ['$0', 'origination'], ['30 yr', 'term']],
    [['7.4%', 'variable'], ['$250k', 'line up to'], ['10 yr', 'draw']],
  ],
  investment: [
    [['0.25%', 'fee'], ['$100', 'to start'], ['Auto', 'rebalance']],
    [['Tax', 'advantaged'], ['$0', 'to open'], ['1:1', 'advisor']],
    [['Dedicated', 'advisor'], ['$250k', 'minimum'], ['0.6%', 'fee']],
  ],
};
export const termsFor = (o: Offer): [string, string][] =>
  TERMS[o.product_category]?.[clamp(Math.round(o.tier_requirement) - 1, 0, 2)] ?? [];

// Short product name for card art: offer_text up to the first ' with ' / ' up to ' / ' that '.
export function cardName(text: string): string {
  const s = text ?? '';
  let cut = s.length;
  for (const sep of [' with ', ' up to ', ' that ']) {
    const i = s.indexOf(sep);
    if (i > 0 && i < cut) cut = i;
  }
  return s.slice(0, cut).trim() || s;
}

export const PAGES: Record<Category, { crumb: string; title: string; sub: string; list: string; stats: [string, string][]; calc?: boolean }> = {
  personal_loan: { crumb: 'Borrow · Personal Loans', title: 'Personal loans with a fixed rate and no surprises', sub: 'Consolidate debt or fund a big moment. Check your rate in two minutes without affecting your credit score.', list: 'Compare personal loans', stats: [['8.99%', 'APR from'], ['$50k', 'borrow up to'], ['1 day', 'funding']], calc: true },
  credit_card: { crumb: 'Cards · All Credit Cards', title: 'Cards that pay you back, on everything', sub: 'Cashback, travel miles, or a first card to build credit. Find the one that fits how you spend.', list: 'Compare credit cards', stats: [['3%', 'cashback'], ['$0', 'foreign fees'], ['3x', 'travel miles']] },
  savings: { crumb: 'Save · Savings & CDs', title: 'Make idle cash work harder', sub: 'High-yield savings and CDs, FDIC insured, no monthly fees.', list: 'Compare savings accounts', stats: [['4.5%', 'APY'], ['$0', 'minimum'], ['FDIC', 'insured']] },
  mortgage: { crumb: 'Home · Mortgages', title: 'Your home, financed on your terms', sub: 'Fixed-rate mortgages and home-equity lines with a dedicated loan officer.', list: 'Compare home loans', stats: [['6.1%', '30-yr fixed'], ['$0', 'origination*'], ['21 days', 'avg close']] },
  investment: { crumb: 'Invest · Wealth & Retirement', title: 'Invest for the long run, with less effort', sub: 'Robo portfolios, IRAs, and private advisory as your wealth grows.', list: 'Compare investing options', stats: [['0.25%', 'mgmt fee'], ['IRA', 'tax-advantaged'], ['24/7', 'rebalancing']] },
};

export const ACTION_TEXT: Record<string, string> = {
  page_view: 'opened',
  product_view: 'looked at',
  calculator_use: 'used the',
  preference_change: 'updated',
  add_to_cart: 'started an application for',
  search: 'searched',
};

export const money = (n: number) => '$' + Math.round(n).toLocaleString();

// ── "Signals behind this match" ───────────────────────────────────────────────
// Built only from what the request actually sent (ctx) and what the profile endpoint returned.
// These are signals the ranker saw, not model attributions.
export type ChipKind = 'req' | 'stream' | 'pos';
export interface Signal {
  label: string;
  kind: ChipKind;
}

const CREDIT_PRODUCTS: Category[] = ['credit_card', 'personal_loan', 'mortgage'];
const CREDIT_WORD: Record<Credit, string> = { excellent: 'Excellent', good: 'Good', fair: 'Fair' };

export function signalsFor(
  o: Offer,
  ctx: RecommendCtx | null,
  profile: { customer: CustomerProfile; categories: Partial<Record<Category, CategoryProfile>> } | null,
  signedIn: boolean,
): Signal[] {
  const out: Signal[] = [];
  const cat = o.product_category;
  const label = CAT[cat]?.label.toLowerCase() ?? cat;
  if (ctx && ctx.goal === cat) out.push({ label: `You're looking to ${GOAL_TEXT[cat]}`, kind: 'req' });
  if (ctx && cat === 'credit_card' && typeof ctx.credit_monthly_spend === 'number') {
    out.push({ label: `${money(ctx.credit_monthly_spend)}/month card spend`, kind: 'req' });
  }
  if (ctx && cat === 'savings' && typeof ctx.savings_deposit === 'number') {
    out.push({ label: `${money(ctx.savings_deposit)} planned deposit`, kind: 'req' });
  }
  if (
    ctx &&
    cat === 'personal_loan' &&
    typeof ctx.loan_amount === 'number' &&
    typeof ctx.loan_term_months === 'number'
  ) {
    out.push({ label: `${money(ctx.loan_amount)} over ${ctx.loan_term_months} months`, kind: 'req' });
  }
  if (ctx && cat === 'mortgage' && typeof ctx.home_price === 'number' && typeof ctx.mortgage_down_payment_pct === 'number') {
    out.push({ label: `${money(ctx.home_price)} home · ${ctx.mortgage_down_payment_pct}% down`, kind: 'req' });
  }
  if (ctx && cat === 'investment' && typeof ctx.investment_amount === 'number') {
    out.push({
      label: `${money(ctx.investment_amount)} to start · ${money(ctx.investment_monthly_contribution ?? 0)}/mo`,
      kind: 'req',
    });
  }
  const web = ctx?.session_cat_views?.[cat];
  if (typeof web === 'number' && web > 0) out.push({ label: `You browsed ${label} this visit`, kind: 'req' });
  const cp = profile?.categories?.[cat];
  const mob = cp?.cust_mobile_cat_views_10m;
  const v30 = cp?.cust_cat_views_30d;
  if (typeof mob === 'number' && mob > 0) out.push({ label: `Looked at ${label} in the mobile app`, kind: 'stream' });
  if (typeof v30 === 'number' && v30 > 0) out.push({ label: `Interested in ${label} this month`, kind: 'pos' });
  const cust = profile?.customer;
  const ti = signedIn ? tierIndex(cust?.cust_loyalty_tier) : -1;
  if (ti >= 0 && ti >= o.tier_requirement - 1 && (o.tier_requirement >= 2 || ti >= 2)) {
    out.push({ label: `${tierLabel(cust?.cust_loyalty_tier)} member`, kind: 'pos' });
  }
  if (ctx && CREDIT_PRODUCTS.includes(cat)) {
    const starterCard = cat === 'credit_card' && o.tier_requirement <= 1;
    if (ctx.credit !== 'fair' || starterCard) out.push({ label: `${CREDIT_WORD[ctx.credit]} credit`, kind: 'req' });
  }
  if (cat === 'credit_card' && typeof cust?.cust_spend_to_income === 'number' && cust.cust_spend_to_income > 0) {
    const r = cust.cust_spend_to_income;
    const pct = r <= 1.5 ? Math.round(r * 100) : Math.round(r);
    out.push({ label: `Card spend is ${pct}% of income`, kind: 'pos' });
  }
  if (ctx && (cat === 'mortgage' || cat === 'investment') && ctx.income >= 100000) {
    out.push({ label: `${money(ctx.income / 1000)}k income`, kind: 'req' });
  }
  if (signedIn && (cat === 'mortgage' || cat === 'investment') && typeof cust?.cust_tenure_months === 'number' && cust.cust_tenure_months >= 24) {
    out.push({ label: `${Math.round(cust.cust_tenure_months / 12)} years with Lakeshore`, kind: 'pos' });
  }
  return out;
}
