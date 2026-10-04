#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
export RESULTS_DIR="${RESULTS_DIR:-benchmark-results/profiled}"
if [ "${PROFILE:-1}" != "0" ]; then
  export TRACE_DIR="$RESULTS_DIR/traces"
fi
exec ./scripts/docker-clusters.sh bench
