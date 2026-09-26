#!/bin/sh
set -eu

mode=${1:-}
submode=${2:-all}
case "$mode" in
  bench) ;;
  test) ;;
  *) echo "usage: $0 bench [standalone|cluster] | test" >&2; exit 2 ;;
esac
case "$submode" in all|standalone|cluster) ;; *) echo "submode must be standalone, cluster, or all" >&2; exit 2 ;; esac

compose="docker compose -p emberdb-bench -f compose.benchmark.yaml"
cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

cleanup
$compose up -d --build

wait_for() {
  host=$1
  attempts=0
  until $compose exec -T benchmark-client redis-cli -h "$host" -p 6379 PING >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ]; then
      echo "timed out waiting for $host" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for ember-standalone
wait_for ember-1
wait_for ember-2
wait_for ember-3
wait_for redis-standalone
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
    EMBER_CLUSTER_ADDR=$ember_1 REDIS_CLUSTER_ADDR=$redis_1 go test ./integration -count=1 -v
    ;;
  bench)
    results=${RESULTS_DIR:-benchmark-results}
    mkdir -p "$results"
    $compose run --rm bench-runner go run ./cmd/ember-bench -mode "$submode" -out "$results/results.json" \
      -requests "${REQUESTS:-2000}" -warmup "${WARMUP:-200}"

    cleanup
    cp scripts/bench-dashboard.html "$results/index.html"
    dashboard_port=${DASHBOARD_PORT:-8080}
    command -v python3 >/dev/null 2>&1 || { echo "python3 is required for the dashboard" >&2; exit 0; }
    echo "Dashboard: http://127.0.0.1:$dashboard_port/ (Ctrl+C to stop)"
    if command -v open >/dev/null 2>&1; then (sleep 1; open "http://127.0.0.1:$dashboard_port/") >/dev/null 2>&1 & fi
    python3 -m http.server "$dashboard_port" --bind 127.0.0.1 --directory "$results"
    ;;
esac
