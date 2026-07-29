#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
SHARED_RUNNER="${REPO_ROOT}/benchmarks/benchbase-spark/run-benchbase-spark.sh"

if [[ ! -f "${SHARED_RUNNER}" ]]; then
  echo "Shared BenchBase runner not found: ${SHARED_RUNNER}" >&2
  exit 2
fi

export ARROWFLIGHT_SOURCE_DIR="TEST/hadoop-arrowflight"
export ARROWFLIGHT_IMAGE="${ARROWFLIGHT_IMAGE:-arrowflight-legacy-test:latest}"
export SPARK_SESSION_CATALOG_CLASS="net.surpin.data.arrowflight.client.spark.LegacyFlightSessionCatalog"
export BENCHBASE_VARIANT="${BENCHBASE_VARIANT:-legacy}"
export BENCHBASE_RESULTS_ROOT="${BENCHBASE_RESULTS_ROOT:-${SCRIPT_DIR}/results}"
export BENCHBASE_RESULTS_HOST_DIR="${BENCHBASE_RESULTS_HOST_DIR:-./TEST/hadoop-arrowflight/benchmarks/benchbase-spark/results}"
export BENCHBASE_UPDATE_PAGES="${BENCHBASE_UPDATE_PAGES:-false}"

# These values are fixed in the legacy implementation and must be recorded as
# effective settings instead of pretending that the newer environment knobs apply.
export FLIGHT_BATCH_SIZE_EFFECTIVE=65536
export FLIGHT_DUCKDB_THREADS_EFFECTIVE=1
export FLIGHT_TIMING_LOG_LEVEL_EFFECTIVE=unsupported-by-legacy

if [[ "${FLIGHT_BATCH_SIZE:-65536}" != "65536" ]]; then
  echo "WARNING: legacy code fixes the Arrow scan batch at 65536; FLIGHT_BATCH_SIZE=${FLIGHT_BATCH_SIZE} is ignored." >&2
fi
if [[ -n "${FLIGHT_DUCKDB_THREADS:-}" && "${FLIGHT_DUCKDB_THREADS}" != "1" ]]; then
  echo "WARNING: legacy code fixes DuckDB threads at 1; FLIGHT_DUCKDB_THREADS=${FLIGHT_DUCKDB_THREADS} is ignored." >&2
fi
if [[ -n "${FLIGHT_TIMING_LOG_LEVEL:-}" ]]; then
  echo "WARNING: legacy code has no structured TIMING logger; FLIGHT_TIMING_LOG_LEVEL is recorded as unsupported." >&2
fi

exec bash "${SHARED_RUNNER}" "$@"
