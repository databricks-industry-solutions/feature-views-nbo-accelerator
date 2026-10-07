export const PRODUCT_CATEGORIES = ['credit_card', 'savings', 'personal_loan', 'mortgage', 'investment'] as const;
export type ProductCategory = (typeof PRODUCT_CATEGORIES)[number];
export type RecommendationGoal = 'none' | ProductCategory;
export type Credit = 'excellent' | 'good' | 'fair';
export interface RankingContext {
  credit: Credit;
  income: number;
  card_spend: number;
  credit_monthly_spend?: number;
  savings_deposit?: number;
  loan_amount?: number;
  loan_term_months?: number;
  home_price?: number;
  mortgage_down_payment_pct?: number;
  investment_amount?: number;
  investment_monthly_contribution?: number;
}

export function constrainOffersForGoal<T extends { product_category: unknown }>(
  offers: T[],
  goal: string,
): { offers: T[]; requestedGoal: ProductCategory | null } {
  const requestedGoal = PRODUCT_CATEGORIES.includes(goal as ProductCategory) ? (goal as ProductCategory) : null;
  const eligible = requestedGoal ? offers.filter((o) => String(o.product_category) === requestedGoal) : offers;
  if (requestedGoal && eligible.length === 0) throw new Error(`No offers found for selected goal '${requestedGoal}'`);
  return { offers: eligible, requestedGoal };
}

export function assertRecommendationGoal(topCategory: unknown, requestedGoal: ProductCategory | null): void {
  if (requestedGoal && String(topCategory) !== requestedGoal) {
    throw new Error(
      `Recommendation invariant failed: selected goal '${requestedGoal}', top category '${String(topCategory)}'`,
    );
  }
}

// Suitability gate for the second stage of the recommender. The model still scores every offer; this
// gate removes products whose tier does not fit the facts the visitor explicitly declared.
export function preferredTier(category: unknown, ctx: RankingContext): number {
  const cat = String(category);
  const creditTier = ctx.credit === 'excellent' ? 3 : ctx.credit === 'good' ? 2 : 1;
  const incomeTier = ctx.income >= 150000 ? 3 : ctx.income >= 65000 ? 2 : 1;
  const spendTier = ctx.card_spend >= 4000 ? 3 : ctx.card_spend >= 1000 ? 2 : 1;
  if (cat === 'credit_card') {
    const explicitSpend = Number(ctx.credit_monthly_spend);
    if (Number.isFinite(explicitSpend) && explicitSpend >= 0) {
      return explicitSpend >= 4000 ? 3 : explicitSpend >= 1000 ? 2 : 1;
    }
    // Every declared control contributes to card suitability. This is intentionally ordinal rather
    // than a hard minimum: one conservative answer should not erase all other customer context.
    const suitability = (creditTier - 1) + (incomeTier - 1) + (spendTier - 1);
    return suitability <= 1 ? 1 : suitability <= 3 ? 2 : 3;
  }
  if (cat === 'savings') {
    const deposit = Number(ctx.savings_deposit);
    if (Number.isFinite(deposit) && deposit >= 0) return deposit >= 50000 ? 3 : deposit >= 5000 ? 2 : 1;
    return incomeTier;
  }
  if (cat === 'personal_loan') {
    const amount = Number(ctx.loan_amount);
    const term = Number(ctx.loan_term_months);
    const hasLoanRequest = (Number.isFinite(amount) && amount > 0) || (Number.isFinite(term) && term > 0);
    if (hasLoanRequest) {
      // Match the product's capacity to the visitor's explicit request. Tier 1 is the short/small
      // loan family, tier 2 covers longer terms or mid-size borrowing, and tier 3 covers $30k+.
      // Credit and income still reach the model and determine ordering within the suitable family.
      const amountTier = amount > 30000 ? 3 : amount > 15000 ? 2 : 1;
      const termTier = term > 36 ? 2 : 1;
      return Math.max(amountTier, termTier);
    }
    return Math.min(creditTier, incomeTier);
  }
  if (cat === 'mortgage') {
    const price = Number(ctx.home_price);
    const down = Number(ctx.mortgage_down_payment_pct);
    if (Number.isFinite(price) && price > 0) {
      if (price >= 900000) return 3;
      if (Number.isFinite(down) && down < 10) return 1;
      return 2;
    }
    return Math.min(creditTier, incomeTier);
  }
  if (cat === 'investment') {
    const amount = Number(ctx.investment_amount);
    const monthly = Number(ctx.investment_monthly_contribution);
    if ((Number.isFinite(amount) && amount >= 0) || (Number.isFinite(monthly) && monthly >= 0)) {
      if (amount >= 250000) return 3;
      if (amount >= 25000 || monthly >= 1000) return 2;
      return 1;
    }
    return incomeTier;
  }
  return 1;
}

export function constrainOffersForContext<T extends { product_category: unknown; tier_requirement: unknown }>(
  offers: T[],
  ctx: RankingContext,
): T[] {
  const eligible = offers.filter((o) => Number(o.tier_requirement) === preferredTier(o.product_category, ctx));
  return eligible.length ? eligible : offers;
}
