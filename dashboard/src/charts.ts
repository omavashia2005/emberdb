import { Chart, BarController, BarElement, CategoryScale, LinearScale, Tooltip, Legend, type ChartDataset } from 'chart.js';
import { $, casesFor, sameCase, pairFor, samplesFor, metricKeys, metricLabels, products, formatMetric, difference,
  type BenchmarkCase, type Result, type Metric } from './comparison.ts';

Chart.register(BarController, BarElement, CategoryScale, LinearScale, Tooltip, Legend);
Chart.defaults.color = '#B5B5B5';
Chart.defaults.font = { ...Chart.defaults.font, family: '"SF Pro Text", -apple-system, BlinkMacSystemFont, sans-serif', size: 12, weight: 400 };
Chart.defaults.animation = false;

let rows: Result[] = [];
let cases: BenchmarkCase[] = [];
let selected: BenchmarkCase | null = null;
let charts: Chart<'bar', (number | null)[], string>[] = [];
const select = (id: string) => $<HTMLSelectElement>(id);
const button = (id: string) => $<HTMLButtonElement>(id);

function options(id: string, values: (string | number)[], value: string | number | undefined) {
  const element = select(id);
  element.replaceChildren(...values.map(item => new Option(String(item), String(item))));
  element.value = values.some(item => String(item) === String(value)) ? String(value) : String(values[0] ?? '');
  element.disabled = values.length === 0;
}

function syncCase() {
  options('case-command', [...new Set(cases.map(row => row.command))], selected?.command);
  const commands = cases.filter(row => row.command === select('case-command').value);
  options('case-variant', [...new Set(commands.map(row => row.variant))], selected?.variant);
  const variants = commands.filter(row => row.variant === select('case-variant').value);
  options('case-clients', variants.map(row => row.concurrency), selected?.concurrency);
  selected = variants.find(row => String(row.concurrency) === select('case-clients').value) || null;
  renderCharts();
}

export function updateCharts(nextRows: Result[]) {
  rows = nextRows;
  cases = casesFor(rows);
  syncCase();
}

function appendRow(body: HTMLElement, values: string[]) {
  const tr = document.createElement('tr');
  values.forEach(value => { const td = document.createElement('td'); td.textContent = value; tr.append(td); });
  body.append(tr);
}

function draw(id: string, labels: string[], values: (number | null)[][], metric: Metric) {
  const canvas = $<HTMLCanvasElement>(id);
  const datasets: ChartDataset<'bar', (number | null)[]>[] = products.map((product, index) => ({
    label: product, data: values[index], backgroundColor: index === 0 ? '#DEDEDE' : '#777777',
    borderColor: index === 0 ? '#DEDEDE' : '#AAAAAA', borderWidth: 1, borderRadius: 3, maxBarThickness: 48,
  }));
  const unit = metric === 'ops_sec' ? 'ops/sec' : 'ms';
  canvas.setAttribute('aria-label', `${metricLabels[metric]} in ${unit}. ${labels.map((label, i) =>
    `${label}: ${products.map((product, p) => `${product} ${formatMetric(values[p][i], metric)}`).join(', ')}`).join('; ')}`);
  charts.push(new Chart<'bar', (number | null)[], string>(canvas, {
    type: 'bar', data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'bottom', onClick: () => {}, labels: { boxWidth: 10, boxHeight: 10, padding: 18 } },
        tooltip: {
          backgroundColor: '#242424', titleColor: '#E6E6E6', bodyColor: '#E6E6E6', borderColor: '#444444', borderWidth: 1,
          titleFont: { weight: 500 },
          callbacks: { label: context => `${context.dataset.label}: ${formatMetric(context.parsed.y, metric)} ${unit}` },
        },
      },
      scales: {
        x: { grid: { display: false }, border: { display: false }, ticks: { maxRotation: 0 } },
        y: { beginAtZero: true, border: { display: false }, grid: { color: '#2B2B2B' },
          title: { display: true, text: unit }, ticks: { maxTicksLimit: 5, callback: value =>
            Number(value).toLocaleString(undefined, { notation: metric === 'ops_sec' ? 'compact' : 'standard', maximumSignificantDigits: 4 }) } },
      },
    },
  }));
}

export function renderCharts() {
  if ($('charts-view').hidden) return;
  charts.forEach(chart => chart.destroy());
  charts = [];
  const index = selected ? cases.findIndex(row => sameCase(row, selected!)) : -1;
  button('case-previous').disabled = index <= 0;
  button('case-next').disabled = index < 0 || index >= cases.length - 1;
  const scope = select('chart-scope').value;
  const latency = select('latency-metric').value as Metric;
  $('latency-control').hidden = scope === 'case';
  $('chart-grid').hidden = !selected;
  $('comparison-panel').hidden = !selected;
  $('chart-data').hidden = !selected;
  $('comparison-values').replaceChildren();
  $('chart-values').replaceChildren();
  if (!selected) {
    $('case-title').textContent = 'Compare a benchmark case';
    $('case-status').textContent = 'No cases available for this topology.';
    $('chart-notice').textContent = 'Load a report with measurements to compare throughput and latency.';
    return;
  }
  const pair = pairFor(rows, selected);
  $('case-title').textContent = `${selected.command} · ${selected.variant} · ${selected.concurrency} clients`;
  $('case-status').textContent = `Case ${index + 1} of ${cases.length} · ${pair.filter(Boolean).length} of 2 products measured`;
  $('comparison-caption').textContent = `${selected.command} · ${selected.variant} · ${selected.concurrency} clients · Median across runs`;
  const missing = products.filter((_, i) => !pair[i]);
  const samples = samplesFor(rows, selected, scope);
  const notes = [scope === 'clients' ? 'Same command and variant across measured client counts.' : scope === 'runs' ?
    'Individual repeats for the selected case. Run numbers are independent for each product.' : 'Both products use the same command, variant, and client count. Bars start at zero.'];
  if (missing.length && scope !== 'clients') notes.push(`${missing.join(' and ')} not measured for this case.`);
  if (scope === 'runs' && pair.some(row => row && !row.runs?.length)) notes.push('Repeated-run measurements are unavailable for one or both products.');
  if (samples.some(sample => sample.values.some(value => !value))) notes.push('Missing measurements are omitted, never plotted as zero.');
  $('chart-notice').textContent = notes.join(' ');
  $('chart-grid').hidden = samples.length === 0;
  $('latency-title').textContent = scope === 'case' ? 'Latency percentiles' : metricLabels[latency];
  if (samples.length) {
    draw('throughput-chart', samples.map(sample => sample.label), products.map((_, p) => samples.map(sample => sample.values[p]?.ops_sec ?? null)), 'ops_sec');
    if (scope === 'case') {
      const latencyKeys = metricKeys.filter(key => key !== 'ops_sec');
      draw('latency-chart', ['p50', 'p95', 'p99', 'Max'], products.map((_, p) => latencyKeys.map(key => pair[p]?.[key] ?? null)), 'p99_ms');
      $('latency-chart').setAttribute('aria-label', 'p50, p95, p99 and maximum latency in milliseconds for EmberDB and Redis. Exact values follow in the Selected case table.');
    } else {
      draw('latency-chart', samples.map(sample => sample.label), products.map((_, p) => samples.map(sample => sample.values[p]?.[latency] ?? null)), latency);
    }
  }
  for (const metric of metricKeys) {
    const unit = metric === 'ops_sec' ? 'ops/sec' : 'ms';
    appendRow($('comparison-values'), [metricLabels[metric], ...pair.map(row => row ? `${formatMetric(row[metric], metric)} ${unit}` : 'Not measured'), difference(pair[0]?.[metric], pair[1]?.[metric], metric)]);
  }
  for (const sample of samples) {
    products.forEach((product, p) => appendRow($('chart-values'), [sample.label, product, ...metricKeys.map(metric => formatMetric(sample.values[p]?.[metric], metric))]));
  }
}

['case-command', 'case-variant', 'case-clients'].forEach(id => select(id).addEventListener('change', () => {
  selected = { command: select('case-command').value, variant: select('case-variant').value, concurrency: Number(select('case-clients').value) };
  syncCase();
}));
['case-previous', 'case-next'].forEach(id => button(id).addEventListener('click', () => {
  const index = selected ? cases.findIndex(row => sameCase(row, selected!)) : -1;
  selected = cases[index + (id === 'case-next' ? 1 : -1)] || selected;
  syncCase();
}));
['chart-scope', 'latency-metric'].forEach(id => select(id).addEventListener('change', renderCharts));
