import { $, validateReport, type Report, type Mode } from './comparison.ts';
import { updateCharts, renderCharts } from './charts.ts';
const tabs = [...document.querySelectorAll<HTMLButtonElement>('[data-mode]')];
let report: Report | null = null;
let mode: Mode = 'standalone';
let page = 0;
const pageSize = 20;
const number = (value: number) => Number.isFinite(value) ? value.toLocaleString() : '—';

function render() {
  const source = report?.[mode] || [];
  const search = $<HTMLInputElement>('search').value.trim().toLowerCase();
  const product = $<HTMLSelectElement>('product').value;
  const concurrency = $<HTMLSelectElement>('concurrency').value;
  const rows = source.filter(row =>
    (!search || `${row.command} ${row.variant} ${row.product}`.toLowerCase().includes(search)) &&
    (!product || row.product === product) && (!concurrency || String(row.concurrency) === concurrency));
  const pages = Math.max(1, Math.ceil(rows.length / pageSize));
  page = Math.min(page, pages - 1);
  const start = page * pageSize;
  const maxOps = source.reduce((max, row) => Math.max(max, row.ops_sec), 0);
  $('rows').replaceChildren();
  for (const row of rows.slice(start, start + pageSize)) {
    const tr = document.createElement('tr');
    const values = [row.command, row.variant, row.product, number(row.concurrency), number(Math.round(row.ops_sec)),
      ...(['p50_ms', 'p95_ms', 'p99_ms', 'max_ms'] as const).map(key => row[key].toFixed(3))];
    values.forEach((value, index) => {
      const td = document.createElement('td');
      td.textContent = value;
      if (index === 2) {
        const label = document.createElement('span');
        label.className = row.product === 'Redis' ? 'product redis' : 'product';
        label.textContent = value;
        td.replaceChildren(label);
      }
      if (index === 4) {
        td.className = row.product === 'Redis' ? 'throughput redis' : 'throughput';
        const track = document.createElement('div');
        track.className = 'bar-track';
        track.setAttribute('aria-hidden', 'true');
        const bar = document.createElement('div');
        bar.className = 'bar';
        bar.style.width = `${maxOps ? row.ops_sec / maxOps * 100 : 0}%`;
        track.appendChild(bar);
        td.appendChild(track);
      }
      tr.appendChild(td);
    });
    $('rows').appendChild(tr);
  }
  if (!rows.length) {
    const td = document.createElement('td');
    td.colSpan = 9;
    td.className = 'empty';
    td.textContent = !report ? 'Results unavailable. Reload to try again.' : source.length ? 'No matching results. Adjust or clear your filters.' : 'No measurements in this report for this topology.';
    const tr = document.createElement('tr');
    tr.appendChild(td);
    $('rows').appendChild(tr);
  }
  $('result-total').textContent = `${rows.length ? start + 1 : 0}–${Math.min(start + pageSize, rows.length)} of ${number(rows.length)} results`;
  $('page-label').textContent = `${page + 1} / ${pages}`;
  $<HTMLButtonElement>('previous').disabled = page === 0;
  $<HTMLButtonElement>('next').disabled = page + 1 >= pages;
  $<HTMLButtonElement>('clear').disabled = !search && !product && !concurrency;
}

function selectMode(nextMode: Mode) {
  mode = nextMode;
  page = 0;
  tabs.forEach(tab => {
    const selected = tab.dataset.mode === mode;
    tab.setAttribute('aria-selected', String(selected));
    tab.tabIndex = selected ? 0 : -1;
  });
  $('results').setAttribute('aria-labelledby', `${mode}-tab`);
  render();
  updateCharts(report?.[mode] || []);
}

async function load() {
  $<HTMLButtonElement>('reload').disabled = true;
  $<HTMLButtonElement>('reload').textContent = 'Loading…';
  $('results').setAttribute('aria-busy', 'true');
  $('status').textContent = 'Loading results.json…';
  try {
    const response = await fetch('results.json', { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    validateReport(data);
    report = data;
    const allRows = [...(report.standalone || []), ...(report.cluster || [])];
    for (const [id, field] of [['requests', 'requests_per_run'], ['repeats', 'repeats'], ['warmup', 'warmup_requests'], ['total-keys', 'total_keys'], ['preload-keys', 'preload_keys'], ['keys-per-group', 'keys_per_group']] as const) {
      $(id).textContent = number(report.config[field]);
    }
    $('result-count').textContent = number(allRows.length);
    $('clients').textContent = report.config.concurrencies.join(', ') || '—';
    tabs.forEach(tab => $(`${tab.dataset.mode}-count`).textContent = number((report![tab.dataset.mode as Mode] || []).length));
    for (const [id, field] of [['product', 'product'], ['concurrency', 'concurrency']] as const) {
      const selected = $<HTMLSelectElement>(id).value;
      $<HTMLSelectElement>(id).length = 1;
      const values = [...new Set(allRows.map(row => row[field]))].sort((a, b) => typeof a === 'number' && typeof b === 'number' ? a - b : String(a).localeCompare(String(b)));
      values.forEach(value => $<HTMLSelectElement>(id).add(new Option(String(value), String(value))));
      $<HTMLSelectElement>(id).value = values.some(value => String(value) === selected) ? selected : '';
    }
    const generated = new Date(report.generated_at);
    $('status').textContent = `Generated ${Number.isNaN(generated.getTime()) ? report.generated_at : generated.toLocaleString()} · Saved report`;
    selectMode((report[mode] || []).length ? mode : (report.standalone || []).length ? 'standalone' : (report.cluster || []).length ? 'cluster' : mode);
  } catch (error) {
    $('status').textContent = `Could not load results.json: ${error instanceof Error ? error.message : String(error)}.${report ? ' Showing the previously loaded report.' : ' Serve this page alongside a benchmark report, then reload.'}`;
    render();
  } finally {
    $<HTMLButtonElement>('reload').disabled = false;
    $<HTMLButtonElement>('reload').textContent = 'Reload results';
    $('results').setAttribute('aria-busy', 'false');
  }
}

tabs.forEach((tab, index) => {
  tab.addEventListener('click', () => selectMode(tab.dataset.mode as Mode));
  tab.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const next = event.key === 'Home' ? tabs[0] : event.key === 'End' ? tabs[tabs.length - 1] : tabs[(index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length];
    next.focus();
    selectMode(next.dataset.mode as Mode);
  });
});
$<HTMLInputElement>('search').addEventListener('input', () => { page = 0; render(); });
['product', 'concurrency'].forEach(id => $(id).addEventListener('change', () => { page = 0; render(); }));
$<HTMLButtonElement>('clear').addEventListener('click', () => { ['search', 'product', 'concurrency'].forEach(id => $<HTMLInputElement | HTMLSelectElement>(id).value = ''); page = 0; render(); });
$<HTMLButtonElement>('previous').addEventListener('click', () => { page--; render(); });
$<HTMLButtonElement>('next').addEventListener('click', () => { page++; render(); });
$<HTMLButtonElement>('reload').addEventListener('click', load);
const viewTabs = [...document.querySelectorAll<HTMLButtonElement>('[data-view]')];
function selectView(tab: HTMLButtonElement) {
  viewTabs.forEach(button => {
    const selected = button === tab;
    button.setAttribute('aria-selected', String(selected));
    button.tabIndex = selected ? 0 : -1;
    $(`${button.dataset.view}-view`).hidden = !selected;
  });
  renderCharts();
}
viewTabs.forEach((tab, index) => {
  tab.addEventListener('click', () => selectView(tab));
  tab.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const next = event.key === 'Home' ? viewTabs[0] : event.key === 'End' ? viewTabs.at(-1)! : viewTabs[1 - index];
    next.focus();
    selectView(next);
  });
});
load();
