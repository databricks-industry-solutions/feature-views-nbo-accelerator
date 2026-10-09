import { test, expect } from '@playwright/test';

// Smoke test for the customer-facing NBO experience.
test('landing renders the Lakeshore Bank experience', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByText('NBO Accelerator', { exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Offers picked for you, not for everyone' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
});
