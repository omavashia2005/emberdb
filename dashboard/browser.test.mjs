import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { chromium } from 'playwright-core';

const base = { command: 'GET', variant: '100% hit', product: 'EmberDB', concurrency: 1, ops_sec: 12345, p50_ms: .12, p95_ms: .2, p99_ms: .3, max_ms: 1 };
const fixture = {
  generated_at: '2026-09-25T12:00:00Z',
  config: { requests_per_run: 2000, repeats: 5, warmup_requests: 200, concurrencies: [1, 10, 50, 100], total_keys: 100000, preload_keys: 50000, keys_per_group: 10 },
  standalone: ['GET', 'SET'].flatMap(command => ['existing', 'new', 'mixed'].flatMap(variant => [1, 10, 50, 100].flatMap(concurrency => ['EmberDB', 'Redis'].map(product => ({
    ...base, command, variant, concurrency, product, ops_sec: product === 'Redis' ? 15000 : 12345,
    runs: [{ ...base, ops_sec: product === 'Redis' ? 14900 : 12000 }, { ...base, ops_sec: product === 'Redis' ? 15100 : 12700 }],
  }))))),
  cluster: [{ ...base, command: 'MGET', variant: '50% hit / 50% miss' }],
};
let report = structuredClone(fixture);
let status = 200;
const html = await readFile(new URL('../scripts/bench-dashboard.html', import.meta.url));
const server = createServer((request, response) => {
  if (request.url === '/results.json') {
    response.writeHead(status, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify(report));
  } else {
    response.writeHead(200, { 'Content-Type': 'text/html' });
    response.end(html);
  }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
let browser;
try {
  browser = await chromium.launch({ executablePath: process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const url = `http://127.0.0.1:${server.address().port}`;
  const ready = () => page.waitForFunction(() => document.querySelector('#results')?.getAttribute('aria-busy') === 'false');
  const reload = async () => { await page.click('#reload'); await ready(); };
  const text = selector => page.locator(selector).textContent();
  await page.goto(url);
  await ready();
  assert.equal(await text('#result-count'), '49');
  assert.equal(await page.locator('#rows tr').count(), 20);
  await page.click('#next');
  await page.click('#next');
  assert.equal(await page.locator('#rows tr').count(), 8);
  assert.equal(await page.locator('#next').isDisabled(), true);
  await page.selectOption('#product', 'Redis');
  assert.equal(await text('#result-total'), '1–20 of 24 results');
  await page.selectOption('#concurrency', '1');
  assert.equal(await text('#result-total'), '1–6 of 6 results');
  await page.fill('#search', 'missing');
  assert.match(await text('#rows'), /No matching/);
  await page.click('#clear');
  // The view tab supports keyboard selection and preserves independent table filters.
  await page.focus('#table-tab');
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator('#charts-tab').getAttribute('aria-selected'), 'true');
  assert.equal(await page.locator('#table-view').isVisible(), false);
  assert.equal(await text('#case-title'), 'GET · existing · 1 clients');
  assert.match(await text('#case-status'), /Case 1 of 24/);
  assert.match(await text('#comparison-values tr:first-child'), /12,345 ops\/sec.*15,000 ops\/sec.*17.7% lower throughput/);
  assert.equal(await page.locator('#comparison-values tr').count(), 5);
  assert.equal(await page.locator('#latency-control').isVisible(), false);
  assert.match(await page.locator('#throughput-chart').getAttribute('aria-label'), /EmberDB 12,345, Redis 15,000/);
  // Confirm Chart.js painted both canvases, beyond their accessible text.
  for (const id of ['throughput-chart', 'latency-chart']) {
    assert.equal(await page.locator(`#${id}`).evaluate(canvas => {
      const pixels = canvas.getContext('2d').getImageData(0, 0, canvas.width, canvas.height).data;
      return pixels.some((value, i) => i % 4 === 3 && value > 0);
    }), true);
  }
  await page.click('#case-next');
  assert.equal(await page.locator('#case-clients').inputValue(), '10');
  await page.click('#case-previous');
  assert.equal(await page.locator('#case-previous').isDisabled(), true);
  await page.selectOption('#case-command', 'SET');
  await page.selectOption('#case-variant', 'new');
  await page.selectOption('#case-clients', '100');
  assert.equal(await text('#case-title'), 'SET · new · 100 clients');
  await page.selectOption('#chart-scope', 'clients');
  assert.equal(await page.locator('#chart-values tr').count(), 8);
  for (const metric of ['p50_ms', 'p95_ms', 'p99_ms', 'max_ms']) {
    await page.selectOption('#latency-metric', metric);
    assert.match(await text('#latency-title'), new RegExp(metric === 'max_ms' ? 'Max' : metric.slice(0, 3)));
  }
  await page.selectOption('#chart-scope', 'runs');
  assert.equal(await page.locator('#chart-values tr').count(), 4);
  assert.match(await text('#chart-values tr:first-child'), /Run 1EmberDB12,000/);
  // Topology changes pair only matching cases and leave the Charts tab selected.
  await page.click('#cluster-tab');
  assert.equal(await page.locator('#charts-view').isVisible(), true);
  assert.match(await text('#case-title'), /MGET/);
  assert.match(await text('#chart-notice'), /Redis not measured/);
  assert.equal(await page.locator('#chart-grid').isVisible(), false); // No repeats were stored.
  await page.selectOption('#chart-scope', 'case');
  assert.match(await text('#comparison-values'), /Not measuredComparison unavailable/);
  assert.match(await page.locator('#throughput-chart').getAttribute('aria-label'), /Redis Not measured/);
  report.cluster[0].variant = '<img src=x onerror=alert(1)>';
  await reload();
  assert.equal(await page.locator('#charts-view img').count(), 0);
  assert.match(await text('#case-title'), /<img/);
  // Invalid nested metrics must not replace a usable report.
  report.cluster[0].runs = [{ ...base, p99_ms: -1 }];
  await reload();
  assert.match(await text('#status'), /Invalid benchmark report.*previously loaded/);
  status = 404;
  await reload();
  assert.match(await text('#status'), /HTTP 404.*previously loaded/);
  status = 200;
  report = { ...fixture, standalone: null, cluster: [] };
  await reload();
  assert.match(await text('#case-status'), /No cases/);
  assert.equal(await page.locator('#chart-grid').isVisible(), false);
  // Load the current report for optional visual inspection; fixtures remain test-only.
  report = process.env.DASHBOARD_REPORT ? JSON.parse(await readFile(process.env.DASHBOARD_REPORT, 'utf8')) : structuredClone(fixture);
  await reload();
  await page.click('#standalone-tab');
  await page.selectOption('#chart-scope', 'case');
  await page.evaluate(() => window.scrollTo(0, 0));
  if (process.env.DASHBOARD_SCREENSHOT) await page.screenshot({ path: process.env.DASHBOARD_SCREENSHOT, fullPage: true });
  for (const width of [768, 390]) {
    await page.setViewportSize({ width, height: 844 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, `Page overflow at ${width}px`);
    await page.selectOption('#chart-scope', 'clients');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.selectOption('#chart-scope', 'case');
  }
  if (process.env.DASHBOARD_SCREENSHOT) await page.screenshot({ path: process.env.DASHBOARD_SCREENSHOT.replace('.png', '-mobile.png'), fullPage: true });
  // Initial failure is recoverable, including the chart view.
  status = 404;
  await page.reload();
  await ready();
  assert.match(await text('#rows'), /Results unavailable/);
  await page.click('#charts-tab');
  assert.match(await text('#case-status'), /No cases/);
  status = 200;
  await reload();
  assert.equal(await page.locator('#chart-grid').isVisible(), true);
  assert.deepEqual(errors, []);
  console.log('Browser checks passed: table, chart rendering, case navigation, all metrics, scaling, repeats, missing data, recovery, keyboard tabs, desktop/mobile.');
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
}
