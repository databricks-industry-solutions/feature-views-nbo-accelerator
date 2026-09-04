import { test, expect } from '@playwright/test';

// Smoke test for the archer-genie-ui chat app.
test('landing renders the Genie-style chat', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'How can I help you?' })).toBeVisible();
  await expect(page.getByPlaceholder(/Ask about budget vs actual/i)).toBeVisible();
  await expect(page.getByRole('button', { name: 'Send' })).toBeVisible();
});
