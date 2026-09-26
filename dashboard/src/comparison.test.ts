import assert from 'node:assert/strict';
import { casesFor, pairFor, samplesFor, difference, validateReport, type Result } from './comparison.ts';

const base: Result = { product: 'EmberDB', command: 'GET', variant: 'hit', concurrency: 1,
  ops_sec: 100, p50_ms: .1, p95_ms: .2, p99_ms: .3, max_ms: .4 };
const rows: Result[] = [
  { ...base, concurrency: 10, ops_sec: 700 },
  { ...base, product: 'Redis', ops_sec: 200, runs: [{ ...base, ops_sec: 195 }, { ...base, ops_sec: 205 }] },
  { ...base, runs: [{ ...base, ops_sec: 90 }] },
  { ...base, command: 'SET', ops_sec: 900 },
];
assert.equal(casesFor(rows).length, 3); // Pair products by case, never by input order.
assert.deepEqual(pairFor(rows, base).map(row => row?.ops_sec), [100, 200]);
assert.deepEqual(samplesFor(rows, base, 'clients').map(sample => sample.values.map(row => row?.ops_sec)), [[100, 200], [700, undefined]]);
assert.deepEqual(samplesFor(rows, base, 'runs').map(sample => sample.values.map(row => row?.ops_sec)), [[90, 195], [undefined, 205]]);
assert.equal(difference(0, 100, 'ops_sec'), '100% lower throughput');
assert.equal(difference(.1, .2, 'p99_ms'), '50% lower latency');
assert.equal(difference(undefined, 100, 'ops_sec'), 'Comparison unavailable');
assert.equal(difference(100, 0, 'ops_sec'), 'Redis baseline is zero');
assert.equal(difference(0, 0, 'p99_ms'), 'Equal');
const report = { generated_at: '2026-09-25', config: { total_keys: 100, preload_keys: 50, keys_per_group: 10, requests_per_run: 100, warmup_requests: 10, repeats: 2, concurrencies: [1, 10] }, standalone: rows, cluster: null };
assert.doesNotThrow(() => validateReport(report));
assert.throws(() => validateReport({ ...report, standalone: [{ ...base, runs: [{ ...base, p99_ms: -1 }] }] }));
assert.throws(() => validateReport({ ...report, standalone: [{ ...base, ops_sec: Infinity }] }));
assert.throws(() => validateReport({ ...report, config: { ...report.config, concurrencies: ['1'] } }));
assert.deepEqual(casesFor([]), []);
console.log('Comparison checks passed: case pairing, missing data, repeats, zero baselines, report validation.');
