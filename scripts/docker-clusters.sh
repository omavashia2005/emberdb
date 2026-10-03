#!/bin/sh
set -eu

mode=${1:-}
case "$mode" in
  bench) ;;
  test) ;;
  *) echo "usage: $0 bench | test" >&2; exit 2 ;;
esac
[ "$#" -eq 1 ] || { echo "usage: $0 bench | test" >&2; exit 2; }

compose="docker compose -p emberdb-bench -f compose.benchmark.yaml"
if [ "$mode" = bench ] && [ -n "${TRACE_DIR:-}" ]; then
  compose="$compose -f compose.redis-profiler.yaml"
fi
cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

cleanup
$compose up -d --build

wait_for() {
  host=$1
  attempts=0
  until $compose exec -T benchmark-client timeout 2 redis-cli -h "$host" -p 6379 PING >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ]; then
      echo "timed out waiting for $host" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for ember-1
wait_for ember-2
wait_for ember-3
wait_for redis-1
wait_for redis-2
wait_for redis-3

ember_1=$($compose port ember-1 6379)
ember_2=$($compose port ember-2 6379)
ember_3=$($compose port ember-3 6379)
redis_1=$($compose port redis-1 6379)

go run ./cmd/ember-cli --create-cluster "$ember_1" "$ember_2" "$ember_3"
$compose exec -T benchmark-client redis-cli --cluster create redis-1:6379 redis-2:6379 redis-3:6379 --cluster-replicas 0 --cluster-yes >/dev/null
sleep 10

case "$mode" in
  test)
    EMBER_CLUSTER_ADDR=$ember_1 REDIS_CLUSTER_ADDR=$redis_1 go test ./tests/integration -count=1 -v
    ;;
  bench)
    results=${RESULTS_DIR:-benchmark-results}
    mkdir -p "$results"
    $compose run --rm bench-runner go run ./cmd/ember-bench -out "$results/results.json" \
      -requests "${REQUESTS:-20000}" -warmup "${WARMUP:-200}" -trace-dir "${TRACE_DIR:-}"

    cleanup
    cp scripts/bench-dashboard.html "$results/index.html"
    dashboard_port=${DASHBOARD_PORT:-8080}
    command -v python3 >/dev/null 2>&1 || { echo "python3 is required for the dashboard" >&2; exit 0; }
    python3 scripts/serve-benchmark.py "$results" "$dashboard_port"
    ;;
esac
