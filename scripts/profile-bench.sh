#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
export RESULTS_DIR="${RESULTS_DIR:-benchmark-results/profiled}"
export TRACE_DIR="$RESULTS_DIR/traces"
export EMBERDB_RESP_WRITE_PROFILE=1
exec ./scripts/docker-clusters.sh bench
