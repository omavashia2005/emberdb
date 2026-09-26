# Benchmark dashboard

TypeScript + Chart.js. The dark SF Pro / neutral styling follows Generic Dashboard UI. Native controls keep the existing standalone dashboard small; Kumo requires a React application. Icons are omitted because no Nucleo icon package is needed.

```sh
npm ci --prefix dashboard
npm run build --prefix dashboard
npm test --prefix dashboard
```

Node 22.18+ and Chrome are required for these development commands. Set `CHROME_PATH` if Chrome is installed elsewhere.

Edit `dashboard/index.html` and `dashboard/src/`. The build emits `scripts/bench-dashboard.html` with Chart.js bundled inside; the existing benchmark launcher copies it into the results directory. Serving benchmark reports does not require Node, npm, or a CDN. To update an already-served default results directory:

```sh
cp scripts/bench-dashboard.html benchmark-results/index.html
```

The Charts tab pairs EmberDB and Redis by topology, command, variant, and client count. Use command/variant/client selectors or Previous/Next case. **This case** shows throughput and all four latency metrics. **Across clients** compares the same workload at each measured client count. **Repeated runs** displays stored individual measurements; the latency selector switches between p50, p95, p99, and max. Missing measurements remain absent, and chart values are also available in accessible tables. Percentage differences use Redis as the baseline; zero baselines are labeled explicitly.
