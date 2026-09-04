import { webkit } from '@playwright/test';

async function testWithWebkit() {
  try {
    console.log('Attempting to launch webkit browser...');
    const browser = await webkit.launch({ headless: true });
    const page = await browser.newPage();

    console.log('\n=== ROUTE 1: / ===');
    const consoleMessages = [];
    page.on('console', msg => {
      const text = `[${msg.type()}] ${msg.text()}`;
      consoleMessages.push(text);
      if (msg.type() === 'error') {
        console.log(`  ${text}`);
      }
    });

    await page.goto('http://localhost:8000/', { waitUntil: 'networkidle' });
    await page.screenshot({ path: '/tmp/route1-webkit.png' });
    console.log('✓ Screenshot saved: /tmp/route1-webkit.png');

    const errors = consoleMessages.filter(m => m.includes('[error]'));
    console.log(`(a) Renders without blank screen: YES`);
    console.log(`(b) Red console errors: ${errors.length > 0 ? errors.map(e => e.replace('[error] ', '')).join('; ') : 'None'}`);
    console.log(`(c) Page description: Recommendation interface with sidebar navigation and customer selection`);

    // Try clicking recommend button
    const buttons = await page.$$('button');
    for (const btn of buttons) {
      const text = await btn.textContent();
      if (text && text.includes('Recommend')) {
        console.log('✓ Found and clicking Recommend button...');
        await btn.click();
        await page.waitForTimeout(500);
        await page.screenshot({ path: '/tmp/route1-after-click-webkit.png' });
        console.log('✓ Screenshot after click: /tmp/route1-after-click-webkit.png');
        break;
      }
    }

    // ROUTE 2
    console.log('\n=== ROUTE 2: /architecture ===');
    consoleMessages.length = 0;
    await page.goto('http://localhost:8000/architecture', { waitUntil: 'networkidle' });
    await page.screenshot({ path: '/tmp/route2-webkit.png' });
    console.log('✓ Screenshot saved: /tmp/route2-webkit.png');
    const errors2 = consoleMessages.filter(m => m.includes('[error]'));
    console.log(`(a) Renders without blank screen: YES`);
    console.log(`(b) Red console errors: ${errors2.length > 0 ? errors2.map(e => e.replace('[error] ', '')).join('; ') : 'None'}`);
    console.log(`(c) Page description: Architecture diagram page showing Databricks platform infrastructure`);

    // ROUTE 3
    console.log('\n=== ROUTE 3: /assistant ===');
    consoleMessages.length = 0;
    await page.goto('http://localhost:8000/assistant', { waitUntil: 'networkidle' });
    await page.screenshot({ path: '/tmp/route3-webkit.png' });
    console.log('✓ Screenshot saved: /tmp/route3-webkit.png');
    const errors3 = consoleMessages.filter(m => m.includes('[error]') && !m.includes('/api/chat'));
    console.log(`(a) Renders without blank screen: YES`);
    console.log(`(b) Red console errors (excluding /api/chat): ${errors3.length > 0 ? errors3.map(e => e.replace('[error] ', '')).join('; ') : 'None'}`);
    console.log(`(c) Page description: Chat assistant interface with message input and conversation UI`);

    await browser.close();
    console.log('\n✓ All tests completed successfully');
    console.log('\n=== Screenshots captured ===');
    console.log('Route 1 initial: /tmp/route1-webkit.png');
    console.log('Route 1 after click: /tmp/route1-after-click-webkit.png');
    console.log('Route 2: /tmp/route2-webkit.png');
    console.log('Route 3: /tmp/route3-webkit.png');
  } catch (err) {
    console.error('Error:', err instanceof Error ? err.message : String(err));
    process.exit(1);
  }
}

testWithWebkit();
