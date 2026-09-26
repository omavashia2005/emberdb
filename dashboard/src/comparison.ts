export const metricKeys = ['ops_sec', 'p50_ms', 'p95_ms', 'p99_ms', 'max_ms'] as const;
export type Metric = typeof metricKeys[number];
export type Measurements = Record<Metric, number>;
export type Mode = 'standalone' | 'cluster';
export interface BenchmarkCase { command: string; variant: string; concurrency: number }
export interface Result extends BenchmarkCase, Measurements { product: string; runs?: Measurements[] }
export interface Report {
  generated_at: string;
  config: {
    total_keys: number; preload_keys: number; keys_per_group: number; concurrencies: number[];
    requests_per_run: number; warmup_requests: number; repeats: number;
  };
  standalone?: Result[] | null;
  cluster?: Result[] | null;
}
export const metricLabels: Record<Metric, string> = {
  ops_sec: 'Throughput', p50_ms: 'p50 latency', p95_ms: 'p95 latency', p99_ms: 'p99 latency', max_ms: 'Max latency',
};
export const products = ['EmberDB', 'Redis'] as const;

export function $<T extends HTMLElement = HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing dashboard element: ${id}`);
  return element as T;
}

const object = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null;
const nonnegative = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const measurements = (value: unknown): boolean => object(value) && metricKeys.every(key => nonnegative(value[key]));
export function validateReport(value: unknown): asserts value is Report {
  if (!object(value) || typeof value.generated_at !== 'string' || !object(value.config)) throw new Error('Invalid benchmark report');
  const config = value.config;
  if (!['total_keys', 'preload_keys', 'keys_per_group', 'requests_per_run', 'warmup_requests', 'repeats'].every(key => nonnegative(config[key])) ||
    !Array.isArray(config.concurrencies) || !config.concurrencies.every(nonnegative) ||
    !['standalone', 'cluster'].every(mode => value[mode] == null || (Array.isArray(value[mode]) && value[mode].every((row: unknown) =>
      object(row) && ['command', 'variant', 'product'].every(key => typeof row[key] === 'string') && nonnegative(row.concurrency) && measurements(row) &&
      (row.runs == null || (Array.isArray(row.runs) && row.runs.every(measurements))))))) {
    throw new Error('Invalid benchmark report');
  }
}

export function casesFor(rows: Result[]): BenchmarkCase[] {
  const cases = new Map<string, BenchmarkCase>();
  for (const { command, variant, concurrency } of rows) {
    const key = JSON.stringify([command, variant, concurrency]);
    cases.set(key, { command, variant, concurrency });
  }
  return [...cases.values()].sort((a, b) => a.command.localeCompare(b.command) || a.variant.localeCompare(b.variant) || a.concurrency - b.concurrency);
}
export function sameCase(a: BenchmarkCase, b: BenchmarkCase): boolean {
  return a.command === b.command && a.variant === b.variant && a.concurrency === b.concurrency;
}
export function pairFor(rows: Result[], selected: BenchmarkCase): (Result | undefined)[] {
  return products.map(product => rows.find(row => row.product === product && sameCase(row, selected)));
}
export function formatMetric(value: number | null | undefined, metric: Metric): string {
  if (value == null) return 'Not measured';
  return value.toLocaleString(undefined, metric === 'ops_sec' ? { maximumFractionDigits: 0 } : { minimumFractionDigits: 3, maximumFractionDigits: 6 });
}
export function difference(ember: number | undefined, redis: number | undefined, metric: Metric): string {
  if (ember == null || redis == null) return 'Comparison unavailable';
  if (ember === redis) return 'Equal';
  if (redis === 0) return 'Redis baseline is zero';
  const percent = Math.abs((ember - redis) / redis * 100);
  const amount = percent < .1 ? '<0.1%' : `${percent.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  return `${amount} ${ember > redis ? 'higher' : 'lower'}${metric === 'ops_sec' ? ' throughput' : ' latency'}`;
}

export interface ChartSample { label: string; values: (Measurements | undefined)[] }
export function samplesFor(rows: Result[], selected: BenchmarkCase, scope: string): ChartSample[] {
  const pair = pairFor(rows, selected);
  if (scope === 'runs') {
    return Array.from({ length: Math.max(...pair.map(row => row?.runs?.length || 0)) }, (_, index) => ({
      label: `Run ${index + 1}`, values: pair.map(row => row?.runs?.[index]),
    }));
  }
  if (scope === 'clients') {
    return casesFor(rows).filter(row => row.command === selected.command && row.variant === selected.variant)
      .map(row => ({ label: `${row.concurrency} clients`, values: pairFor(rows, row) }));
  }
  return [{ label: `${selected.concurrency} clients`, values: pair }];
}
