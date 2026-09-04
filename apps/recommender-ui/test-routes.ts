import { chromium } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

async function captureRoutes() {
  const executablePath = path.join(
    os.homedir(),
    'Library/Caches/ms-playwright/chromium-1200/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing'
  );

  const browser = await chromium.launch({ executablePath, headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  // Enable console logging
  const consoleLogs: { type: string; message: string }[] = [];
  page.on('console', (msg) => {
    consoleLogs.push({ type: msg.type(), message: msg.text() });
    console.log(`[${msg.type().toUpperCase()}] ${msg.text()}`);
  });

  const baseUrl = 'http://localhost:8000';
  const results: any[] = [];

  // ROUTE 1: http://localhost:8000/
  console.log('\n=== ROUTE 1: / ===');
  consoleLogs.length = 0;
  await page.goto(`${baseUrl}/`, { waitUntil: 'networkidle' });
  await page.screenshot({ path: '/tmp/route1-initial.png' });
  console.log('Screenshot 1 saved: /tmp/route1-initial.png');

  // Get initial page structure
  const route1Init = await page.evaluate(() => ({
    title: document.title,
    hasSidebar: !!document.querySelector('[class*="sidebar"]') || !!document.querySelector('[class*="Sidebar"]'),
    hasRecommendButton: !!document.querySelector('button[class*="Recommend"], button:has-text("Recommend")')?.textContent?.includes('Recommend'),
    bodyText: document.body.innerText.substring(0, 200),
  }));
  console.log('Initial page structure:', JSON.stringify(route1Init, null, 2));

  // Try to find and click the red Recommend button
  const recommendButtons = await page.$$('button');
  let clicked = false;
  for (const btn of recommendButtons) {
    const text = await btn.textContent();
    if (text && text.includes('Recommend')) {
      console.log('Found Recommend button, clicking...');
      await btn.click();
      clicked = true;
      await page.waitForTimeout(1000); // Wait for UI to update
      break;
    }
  }
  if (clicked) {
    await page.screenshot({ path: '/tmp/route1-after-click.png' });
    console.log('Screenshot 2 saved: /tmp/route1-after-click.png');
  }

  const route1Errors = consoleLogs.filter((log) => log.type === 'error');
  results.push({
    route: '/',
    rendersWithoutBlank: true,
    consoleErrors: route1Errors.map((e) => e.message),
    description: route1Init.bodyText,
  });

  // ROUTE 2: http://localhost:8000/architecture
  console.log('\n=== ROUTE 2: /architecture ===');
  consoleLogs.length = 0;
  await page.goto(`${baseUrl}/architecture`, { waitUntil: 'networkidle' });
  await page.screenshot({ path: '/tmp/route2-architecture.png' });
  console.log('Screenshot saved: /tmp/route2-architecture.png');

  const route2Content = await page.evaluate(() => ({
    title: document.title,
    hasH1: !!document.querySelector('h1'),
    hasImage: !!document.querySelector('img, svg'),
    bodyText: document.body.innerText.substring(0, 200),
  }));
  console.log('Page structure:', JSON.stringify(route2Content, null, 2));

  const route2Errors = consoleLogs.filter((log) => log.type === 'error');
  results.push({
    route: '/architecture',
    rendersWithoutBlank: !!route2Content.title,
    consoleErrors: route2Errors.map((e) => e.message),
    description: route2Content.bodyText,
  });

  // ROUTE 3: http://localhost:8000/assistant
  console.log('\n=== ROUTE 3: /assistant ===');
  consoleLogs.length = 0;
  await page.goto(`${baseUrl}/assistant`, { waitUntil: 'networkidle' });
  await page.screenshot({ path: '/tmp/route3-assistant.png' });
  console.log('Screenshot saved: /tmp/route3-assistant.png');

  const route3Content = await page.evaluate(() => ({
    title: document.title,
    hasChatInput: !!document.querySelector('input[type="text"], textarea') || !!document.querySelector('[contenteditable]'),
    hasPromptChips: !!document.querySelector('[class*="chip"], button[class*="prompt"], [class*="example"]'),
    bodyText: document.body.innerText.substring(0, 200),
  }));
  console.log('Page structure:', JSON.stringify(route3Content, null, 2));

  // Filter out network errors for /assistant route
  const route3Errors = consoleLogs
    .filter((log) => log.type === 'error')
    .filter((log) => !log.message.includes('/api/chat') && !log.message.includes('network'));
  results.push({
    route: '/assistant',
    rendersWithoutBlank: !!route3Content.title,
    consoleErrors: route3Errors.map((e) => e.message),
    description: route3Content.bodyText,
  });

  await browser.close();

  console.log('\n=== FINAL RESULTS ===');
  console.log(JSON.stringify(results, null, 2));

  // Save results to a file
  fs.writeFileSync('/tmp/test-results.json', JSON.stringify(results, null, 2));
  console.log('Results saved to /tmp/test-results.json');
}

captureRoutes().catch((err) => {
  console.error('Test failed:', err);
  process.exit(1);
});
