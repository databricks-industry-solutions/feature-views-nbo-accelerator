import { describe, expect, it } from 'vitest';
import {
  PRODUCT_CATEGORIES,
  assertRecommendationGoal,
  constrainOffersForContext,
  constrainOffersForGoal,
  preferredTier,
} from './recommendation.js';

const catalog = PRODUCT_CATEGORIES.flatMap((product_category) =>
  Array.from({ length: 8 }, (_, i) => ({ offer_id: `${product_category}_${i}`, product_category })),
);

describe('OfferMatch goal contract', () => {
  it.each(PRODUCT_CATEGORIES)('%s returns only the selected category', (goal) => {
    const result = constrainOffersForGoal(catalog, goal);
    expect(result.offers).toHaveLength(8);
    expect(new Set(result.offers.map((o) => o.product_category))).toEqual(new Set([goal]));
    expect(() => assertRecommendationGoal(result.offers[0].product_category, result.requestedGoal)).not.toThrow();
  });

  it('none keeps the full personalized catalog', () => {
    const result = constrainOffersForGoal(catalog, 'none');
    expect(result.offers).toHaveLength(40);
    expect(result.requestedGoal).toBeNull();
  });

  it('rejects a mismatched top category', () => {
    expect(() => assertRecommendationGoal('credit_card', 'mortgage')).toThrow(/selected goal 'mortgage'/);
  });

  it('credit changes the top credit-card tier', () => {
    expect(preferredTier('credit_card', { credit: 'fair', income: 85000, card_spend: 1500 })).toBe(2);
    expect(preferredTier('credit_card', { credit: 'excellent', income: 85000, card_spend: 1500 })).toBe(3);
  });

  it('card spend changes the top credit-card tier', () => {
    expect(preferredTier('credit_card', { credit: 'good', income: 85000, card_spend: 250 })).toBe(2);
    expect(preferredTier('credit_card', { credit: 'good', income: 85000, card_spend: 7000 })).toBe(3);
  });

  it('income changes the top credit-card tier', () => {
    expect(preferredTier('credit_card', { credit: 'good', income: 45000, card_spend: 1500 })).toBe(2);
    expect(preferredTier('credit_card', { credit: 'good', income: 250000, card_spend: 1500 })).toBe(3);
  });

  it('income changes the top mortgage tier', () => {
    expect(preferredTier('mortgage', { credit: 'good', income: 45000, card_spend: 1500 })).toBe(1);
    expect(preferredTier('mortgage', { credit: 'good', income: 250000, card_spend: 1500 })).toBe(2);
  });

  it('credit-card page spend changes the suitable family', () => {
    const base = { credit: 'good' as const, income: 85000, card_spend: 1500 };
    expect(preferredTier('credit_card', { ...base, credit_monthly_spend: 250 })).toBe(1);
    expect(preferredTier('credit_card', { ...base, credit_monthly_spend: 2000 })).toBe(2);
    expect(preferredTier('credit_card', { ...base, credit_monthly_spend: 6000 })).toBe(3);
  });

  it('savings deposit changes the suitable family', () => {
    const base = { credit: 'good' as const, income: 85000, card_spend: 1500 };
    expect(preferredTier('savings', { ...base, savings_deposit: 1000 })).toBe(1);
    expect(preferredTier('savings', { ...base, savings_deposit: 10000 })).toBe(2);
    expect(preferredTier('savings', { ...base, savings_deposit: 100000 })).toBe(3);
  });

  it('home price and down payment each change the mortgage family', () => {
    const base = { credit: 'good' as const, income: 85000, card_spend: 1500 };
    expect(preferredTier('mortgage', { ...base, home_price: 500000, mortgage_down_payment_pct: 5 })).toBe(1);
    expect(preferredTier('mortgage', { ...base, home_price: 500000, mortgage_down_payment_pct: 20 })).toBe(2);
    expect(preferredTier('mortgage', { ...base, home_price: 1200000, mortgage_down_payment_pct: 20 })).toBe(3);
  });

  it('investment amount and contribution each change the investment family', () => {
    const base = { credit: 'good' as const, income: 85000, card_spend: 1500 };
    expect(preferredTier('investment', { ...base, investment_amount: 5000, investment_monthly_contribution: 250 })).toBe(1);
    expect(preferredTier('investment', { ...base, investment_amount: 5000, investment_monthly_contribution: 1500 })).toBe(2);
    expect(preferredTier('investment', { ...base, investment_amount: 300000, investment_monthly_contribution: 250 })).toBe(3);
  });

  it('loan amount changes the top personal-loan family', () => {
    const scored = [
      { offer_id: 'small', product_category: 'personal_loan', tier_requirement: 1, score: 0.99 },
      { offer_id: 'flexible', product_category: 'personal_loan', tier_requirement: 2, score: 0.80 },
      { offer_id: 'premier', product_category: 'personal_loan', tier_requirement: 3, score: 0.70 },
    ];
    const base = { credit: 'good' as const, income: 85000, card_spend: 1500, loan_term_months: 24 };
    expect(constrainOffersForContext(scored, { ...base, loan_amount: 10000 })[0]?.offer_id).toBe('small');
    expect(constrainOffersForContext(scored, { ...base, loan_amount: 50000 })[0]?.offer_id).toBe('premier');
  });

  it('loan term changes the top personal-loan family', () => {
    const scored = [
      { offer_id: 'small', product_category: 'personal_loan', tier_requirement: 1, score: 0.99 },
      { offer_id: 'flexible', product_category: 'personal_loan', tier_requirement: 2, score: 0.80 },
      { offer_id: 'premier', product_category: 'personal_loan', tier_requirement: 3, score: 0.70 },
    ];
    const base = { credit: 'good' as const, income: 85000, card_spend: 1500, loan_amount: 10000 };
    expect(constrainOffersForContext(scored, { ...base, loan_term_months: 24 })[0]?.offer_id).toBe('small');
    expect(constrainOffersForContext(scored, { ...base, loan_term_months: 60 })[0]?.offer_id).toBe('flexible');
  });

  it('filters scored offers to the suitable tier while preserving model order', () => {
    const scored = [
      { product_category: 'credit_card', tier_requirement: 1, score: 0.99 },
      { product_category: 'credit_card', tier_requirement: 2, score: 0.80 },
      { product_category: 'credit_card', tier_requirement: 2, score: 0.70 },
      { product_category: 'credit_card', tier_requirement: 3, score: 0.60 },
      { product_category: 'credit_card', tier_requirement: 3, score: 0.50 },
    ];
    const result = constrainOffersForContext(scored, { credit: 'excellent', income: 85000, card_spend: 1500 });
    expect(result.map((o) => o.score)).toEqual([0.6, 0.5]);
  });
});
