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

// ── Demo customers ────────────────────────────────────────────────
export const DEMO_CUSTOMERS: Customer[] = [
  { customer_id: 'cust_10428', loyalty_tier: 'Platinum', risk_band: 'Low', annual_income: 245000, tenure_months: 96 },
  { customer_id: 'cust_20913', loyalty_tier: 'Gold', risk_band: 'Low', annual_income: 138000, tenure_months: 54 },
  { customer_id: 'cust_33027', loyalty_tier: 'Gold', risk_band: 'Medium', annual_income: 91000, tenure_months: 33 },
  { customer_id: 'cust_41185', loyalty_tier: 'Silver', risk_band: 'Medium', annual_income: 72000, tenure_months: 21 },
  { customer_id: 'cust_55640', loyalty_tier: 'Silver', risk_band: 'High', annual_income: 58000, tenure_months: 12 },
  { customer_id: 'cust_67291', loyalty_tier: 'Bronze', risk_band: 'Low', annual_income: 64000, tenure_months: 8 },
  { customer_id: 'cust_78550', loyalty_tier: 'Bronze', risk_band: 'High', annual_income: 39000, tenure_months: 5 },
  { customer_id: 'cust_84102', loyalty_tier: 'Platinum', risk_band: 'Medium', annual_income: 310000, tenure_months: 120 },
];

// ── Demo offer catalog (offer_text mirrors the accelerator's nb01) ─
export const DEMO_OFFERS: Offer[] = [
  { offer_id: 'offer_0', product_category: 'credit_card', offer_text: 'Premium Rewards Credit Card with 3% cashback on all purchases', base_reward: 84.2, tier_requirement: 3 },
  { offer_id: 'offer_5', product_category: 'credit_card', offer_text: 'Travel Credit Card with airline miles and no foreign transaction fees', base_reward: 71.5, tier_requirement: 2 },
  { offer_id: 'offer_9', product_category: 'credit_card', offer_text: 'Business Credit Card with expense tracking and cashback rewards', base_reward: 66.0, tier_requirement: 2 },
  { offer_id: 'offer_1', product_category: 'savings', offer_text: 'High-Yield Savings Account with 4.5% APY and no minimum balance', base_reward: 45.0, tier_requirement: 1 },
  { offer_id: 'offer_6', product_category: 'savings', offer_text: 'Student Checking Account with no monthly fees and overdraft protection', base_reward: 22.0, tier_requirement: 1 },
  { offer_id: 'offer_2', product_category: 'personal_loan', offer_text: 'Personal Loan up to 50k with fixed low APR and flexible terms', base_reward: 58.5, tier_requirement: 2 },
  { offer_id: 'offer_3', product_category: 'mortgage', offer_text: '30-Year Fixed Mortgage with competitive rates and no origination fee', base_reward: 92.0, tier_requirement: 4 },
  { offer_id: 'offer_7', product_category: 'mortgage', offer_text: 'Home Equity Line of Credit with variable rate and easy access', base_reward: 77.0, tier_requirement: 3 },
  { offer_id: 'offer_4', product_category: 'investment', offer_text: 'Diversified Investment Portfolio with robo-advisor and low fees', base_reward: 68.0, tier_requirement: 3 },
  { offer_id: 'offer_8', product_category: 'investment', offer_text: 'Retirement IRA with tax advantages and employer matching guidance', base_reward: 61.0, tier_requirement: 2 },
];

const TIER_RANK: Record<Customer['loyalty_tier'], number> = { Bronze: 1, Silver: 2, Gold: 3, Platinum: 4 };
const RISK_PENALTY: Record<Customer['risk_band'], number> = { Low: 0, Medium: 0.15, High: 0.35 };

// Deterministic mock ranker: a plausible P(accept) from tier/income/reward/risk.
// Swapped for the live serving-endpoint call when data is wired.
export function mockRank(customer: Customer, offers: Offer[]): RankedOffer[] {
  const tier = TIER_RANK[customer.loyalty_tier];
  const incomeF = Math.min(1, customer.annual_income / 250000);
  const tenureF = Math.min(1, customer.tenure_months / 120);
  return offers
    .map((o) => {
      const eligible = tier >= o.tier_requirement ? 1 : 0.35;
      const rewardF = o.base_reward / 100;
      let logit =
        -0.4 +
        1.8 * rewardF +
        1.1 * incomeF +
        0.6 * tenureF +
        0.5 * (tier / 4) -
        1.4 * (o.tier_requirement / 4) -
        3 * RISK_PENALTY[customer.risk_band];
      // category affinity nudges keyed off profile
      if (o.product_category === 'mortgage' && incomeF > 0.5) logit += 0.6;
      if (o.product_category === 'investment' && tier >= 3) logit += 0.5;
      if (o.product_category === 'credit_card') logit += 0.3;
      if (o.product_category === 'savings' && customer.risk_band === 'High') logit += 0.4;
      const p = eligible * (1 / (1 + Math.exp(-logit)));
      return { ...o, score: Math.max(0.02, Math.min(0.99, p)) };
    })
    .sort((a, b) => b.score - a.score);
}

export const fmtUSD = (n: number) =>
  n >= 1000 ? `$${(n / 1000).toFixed(n >= 100000 ? 0 : 1)}k` : `$${n.toFixed(0)}`;
