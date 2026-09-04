// NBO domain model + demo data.
//
// Shapes mirror the real Delta/online tables so wiring live data is a drop-in:
//   customers(customer_id, loyalty_tier, risk_band, annual_income, tenure_months)
//   offers(offer_id, product_category, offer_text, base_reward, tier_requirement)
// The ranker returns one acceptance probability per offer for a customer.

export type ProductCategory =
  | 'credit_card'
  | 'savings'
  | 'personal_loan'
  | 'mortgage'
  | 'investment';

export interface Customer {
  customer_id: string;
  loyalty_tier: 'Bronze' | 'Silver' | 'Gold' | 'Platinum';
  risk_band: 'Low' | 'Medium' | 'High';
  annual_income: number;
  tenure_months: number;
}

export interface Offer {
  offer_id: string;
  product_category: ProductCategory;
  offer_text: string;
  base_reward: number;
  tier_requirement: number;
}

export interface RankedOffer extends Offer {
  score: number; // P(accept), 0..1
}

// SQL results (JSON_ARRAY) come back as strings; coerce to the typed shapes the UI expects.
// Tier/risk may arrive lower- or mixed-case from the data — Title-case them to match the UI maps.
const titleCase = (s: string) => (s ? s[0].toUpperCase() + s.slice(1).toLowerCase() : s);
export function normalizeCustomer(r: Record<string, unknown>): Customer {
  return {
    customer_id: String(r.customer_id),
    loyalty_tier: titleCase(String(r.loyalty_tier)) as Customer['loyalty_tier'],
    risk_band: titleCase(String(r.risk_band)) as Customer['risk_band'],
    annual_income: Number(r.annual_income),
    tenure_months: Number(r.tenure_months),
  };
}

export function normalizeRankedOffer(r: unknown): RankedOffer {
  const o = r as Record<string, unknown>;
  return {
    offer_id: String(o.offer_id),
    product_category: String(o.product_category) as ProductCategory,
    offer_text: String(o.offer_text ?? ''),
    base_reward: Number(o.base_reward),
    tier_requirement: Number(o.tier_requirement),
    score: Number(o.score),
  };
}

// ── Category presentation: gradient, accent, label ────────────────
export const CATEGORY_META: Record<
  ProductCategory,
  { label: string; gradient: string; accent: string; blurb: string }
> = {
  credit_card: {
    label: 'Credit Card',
    gradient: 'linear-gradient(135deg,#ff3621 0%,#b4200f 100%)',
    accent: '#ff3621',
    blurb: 'Rewards & cashback',
  },
  savings: {
    label: 'Savings',
    gradient: 'linear-gradient(135deg,#14507e 0%,#0b2e4f 100%)',
    accent: '#14507e',
    blurb: 'High-yield deposits',
  },
  personal_loan: {
    label: 'Personal Loan',
    gradient: 'linear-gradient(135deg,#7c3aed 0%,#4c1d95 100%)',
    accent: '#7c3aed',
    blurb: 'Flexible financing',
  },
  mortgage: {
    label: 'Mortgage',
    gradient: 'linear-gradient(135deg,#0f9d76 0%,#065f46 100%)',
    accent: '#0f9d76',
    blurb: 'Home lending',
  },
  investment: {
    label: 'Investment',
    gradient: 'linear-gradient(135deg,#d97706 0%,#92400e 100%)',
    accent: '#d97706',
    blurb: 'Wealth & retirement',
  },
};

// Richer marketing/product detail per category, for the offer detail view.
export const CATEGORY_DETAIL: Record<
  ProductCategory,
  { overview: string; features: string[]; idealFor: string; terms: string }
> = {
  credit_card: {
    overview: 'A rewards credit card that earns on everyday spend, with no annual fee for eligible tiers.',
    features: ['Cashback / points on all purchases', 'No foreign transaction fees', 'Fraud protection & instant lock', 'Digital wallet ready'],
    idealFor: 'Customers with steady spend who pay balances monthly and value rewards.',
    terms: 'Variable APR by creditworthiness · reward rate shown as base_reward · subject to approval.',
  },
  savings: {
    overview: 'A high-yield deposit account that pays competitive interest with easy access to funds.',
    features: ['4.5% APY, no minimum balance', 'FDIC insured', 'Automatic savings rules', 'No monthly fees'],
    idealFor: 'Customers building an emergency fund or holding idle cash in low-yield checking.',
    terms: 'APY variable · rate may change after account opening · no lock-up period.',
  },
  personal_loan: {
    overview: 'An unsecured installment loan with a fixed rate and predictable monthly payments.',
    features: ['Borrow up to $50k', 'Fixed APR & term', 'No prepayment penalty', 'Funds in as little as 1 day'],
    idealFor: 'Customers consolidating higher-rate debt or financing a large one-time expense.',
    terms: 'Fixed APR by risk band · 24–60 month terms · subject to income verification.',
  },
  mortgage: {
    overview: 'Home financing — from a 30-year fixed purchase mortgage to a flexible home-equity line.',
    features: ['Competitive fixed rates', 'No origination fee (select tiers)', 'HELOC option for equity access', 'Dedicated loan officer'],
    idealFor: 'Higher-income, longer-tenure customers buying, refinancing, or tapping home equity.',
    terms: 'Rate + points by profile · appraisal & underwriting required · property as collateral.',
  },
  investment: {
    overview: 'A managed, diversified portfolio or tax-advantaged retirement account with low fees.',
    features: ['Robo-advisor allocation', 'Low management fee', 'Tax-advantaged IRA option', 'Automatic rebalancing'],
    idealFor: 'Customers with investable assets seeking long-term growth and retirement planning.',
    terms: 'Not FDIC insured · may lose value · advisory fee applies · not financial advice.',
  },
};

export const fmtUSD = (n: number) =>
  n >= 1000 ? `$${(n / 1000).toFixed(n >= 100000 ? 0 : 1)}k` : `$${n.toFixed(0)}`;
