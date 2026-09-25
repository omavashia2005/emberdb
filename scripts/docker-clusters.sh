#!/bin/sh
set -eu

mode=${1:-}
case "$mode" in
  bench|test) ;;
  *) echo "usage: $0 bench|test" >&2; exit 2 ;;
esac

compose="docker compose -p emberdb-bench -f compose.benchmark.yaml"
cleanup() {
  $compose down -v --remove-orphans >/dev/null 2>&1 || true
}
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

ember_standalone=$($compose port ember-standalone 6379)
ember_1=$($compose port ember-1 6379)
ember_2=$($compose port ember-2 6379)
ember_3=$($compose port ember-3 6379)
redis_standalone=$($compose port redis-standalone 6379)
redis_1=$($compose port redis-1 6379)
redis_2=$($compose port redis-2 6379)
redis_3=$($compose port redis-3 6379)

go run ./cmd/ember-cli --create-cluster "$ember_1" "$ember_2" "$ember_3"
$compose exec -T benchmark-client redis-cli --cluster create redis-1:6379 redis-2:6379 redis-3:6379 --cluster-replicas 0 --cluster-yes >/dev/null
sleep 10

if [ "$mode" = test ]; then
  EMBER_CLUSTER_ADDR=$ember_1 REDIS_CLUSTER_ADDR=$redis_1 go test ./integration -count=1 -v
else
  go run ./cmd/ember-bench \
    -requests "${REQUESTS:-100000}" \
    -clients "${CLIENTS:-50}" \
    -ember-standalone "$ember_standalone" \
    -redis-standalone "$redis_standalone" \
    -ember-cluster "$ember_1,$ember_2,$ember_3" \
    -redis-cluster "$redis_1,$redis_2,$redis_3"
fi
