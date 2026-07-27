#!/usr/bin/env bash
set -euo pipefail

BENCHMARK="${1:-tpch}"
MODE="${2:-smoke}"
QUERY_SET="${3:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"
RESULTS_ROOT="${SCRIPT_DIR}/results"
PAGES_DIR="${REPO_ROOT}/pages"
RESULTS_RUN_ID="${BENCHBASE_RESULTS_ID:-}"
RESULTS_DIR="${RESULTS_ROOT}"
CONFIG_DIR="${SCRIPT_DIR}/config"
RESULTS_IN_CONTAINER="/benchbase/results"
GENERATED_CONFIG_LOCAL=""
GENERATED_CONFIGS=()
EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/${BENCHMARK}.xml"
FLIGHT_SERVER_SERVICES=()
COMPARE_PARENT_RUN_ID=""
BENCHBASE_TIME_SECONDS="${BENCHBASE_TIME_SECONDS:-}"
BENCHBASE_WARMUP_SECONDS="${BENCHBASE_WARMUP_SECONDS:-}"
BENCHBASE_TERMINALS="${BENCHBASE_TERMINALS:-}"
BENCHBASE_QUERY_REPETITIONS="${BENCHBASE_QUERY_REPETITIONS:-1}"
BENCHBASE_RATE="${BENCHBASE_RATE:-unlimited}"
BENCHBASE_DB_SCHEMA="${BENCHBASE_DB_SCHEMA:-}"
BENCHBASE_COMPARE_ORDER="${BENCHBASE_COMPARE_ORDER:-flight-first}"
BENCHBASE_PAIRED_OBSERVATIONS="${BENCHBASE_PAIRED_OBSERVATIONS:-1}"
BENCHBASE_CACHE_POLICY="${BENCHBASE_CACHE_POLICY:-warm-cache}"
BENCHBASE_UPDATE_PAGES="${BENCHBASE_UPDATE_PAGES:-true}"
BENCHBASE_CAPTURE_TIMEOUT_SECONDS="${BENCHBASE_CAPTURE_TIMEOUT_SECONDS:-${BENCHBASE_QUERY_TIMEOUT_SECONDS:-0}}"
BENCHMARK_OBSERVABILITY="${BENCHMARK_OBSERVABILITY:-true}"
BENCHBASE_TIMED_QUERY_COUNT=1
EXECUTION_PATH_LOG_IN_CONTAINER="/tmp/arrowflight-benchmark-execution-paths.jsonl"
export SPARK_SQL_ANSI_ENABLED="${SPARK_SQL_ANSI_ENABLED:-true}"
export FLIGHT_BATCH_SIZE="${FLIGHT_BATCH_SIZE:-65536}"
export FLIGHT_TIMING_LOG_LEVEL="${FLIGHT_TIMING_LOG_LEVEL:-}"
export FLIGHT_DUCKDB_THREADS="${FLIGHT_DUCKDB_THREADS:-}"
export BENCHMARK_GENERATOR_IMAGE="${BENCHMARK_GENERATOR_IMAGE:-arrowflight-duckdb-benchmark-generator:latest}"
BENCHMARK_GENERATOR_BUILD_RETRIES="${BENCHMARK_GENERATOR_BUILD_RETRIES:-4}"
BENCHMARK_REBUILD_GENERATOR="${BENCHMARK_REBUILD_GENERATOR:-false}"
BENCHBASE_IMAGE_READY=false
PYTHON_CMD=()

usage() {
  cat >&2 <<'EOF'
Usage: bash benchmarks/benchbase-spark/run-benchbase-spark.sh <benchmark> <mode> [queries]

Benchmarks:
  tpch
  tpcds

Modes:
  smoke    reset volumes, generate shared HDFS Parquet, register Flight, verify Spark, execute small query set
  graph    reset volumes, run a timed TPC-H query set so the HTML report has chart points
  compare  reset volumes, register Flight and direct HDFS Parquet tables, execute both
  fresh    reset volumes, generate shared HDFS Parquet, register Flight, verify Spark, execute benchmark
  prepare  reset volumes, generate shared HDFS Parquet, register Flight, verify Spark, start Spark Thrift
  prepare-compare
           reset volumes, generate Flight/direct data, register both schemas, start Spark Thrift
  run      execute BenchBase against already prepared Spark Flight tables
  run-flight
           execute BenchBase against <benchmark>_flight on an already prepared compare stack
  run-direct
           execute BenchBase against <benchmark>_direct on an already prepared compare stack
  report   build latest HTML report
  pages    rebuild GitHub Pages dashboard from local results
  down     stop stack and remove Docker volumes
  logs     follow compose logs

Queries:
  TPC-H: q1-q22, comma-separated lists, all
  TPC-DS: q1-q99, comma-separated lists

Serial repetitions:
  BENCHBASE_QUERY_REPETITIONS=N runs every selected query N measured times.
  It cannot be combined with BENCHBASE_TIME_SECONDS.

Paired comparison:
  BENCHBASE_PAIRED_OBSERVATIONS=N schedules N pairs (default: 1).
  BENCHBASE_CLUSTER_NODES=N selects 1-10 Flight/Spark worker nodes.
  BENCHBASE_COMPARE_ORDER=flight-first|direct-first selects the first pair;
  later pairs alternate automatically.
  BENCHBASE_CACHE_POLICY=warm-cache reuses the prepared stack without eviction.

Timed execution:
  BENCHBASE_TIME_SECONDS=N gives every selected query N measured seconds.
  BENCHBASE_WARMUP_SECONDS=N applies once before the first selected query.
EOF
}

compose() {
  local compose_file="${COMPOSE_FILE}"
  if command -v cygpath >/dev/null 2>&1; then
    compose_file="$(cygpath -w "${COMPOSE_FILE}")"
    MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker compose -f "${compose_file}" "$@"
  else
    docker compose -f "${compose_file}" "$@"
  fi
}

try_python() {
  local -a candidate=("$@")
  if command -v "${candidate[0]}" >/dev/null 2>&1 && "${candidate[@]}" --version >/dev/null 2>&1; then
    PYTHON_CMD=("${candidate[@]}")
    return 0
  fi
  return 1
}

detect_python() {
  if try_python python3; then
    return
  elif try_python python; then
    return
  elif try_python py -3; then
    return
  else
    echo "Python is required to generate BenchBase HTML reports." >&2
    exit 2
  fi
}

run_python() {
  if (( ${#PYTHON_CMD[@]} == 0 )); then
    detect_python
  fi
  "${PYTHON_CMD[@]}" "$@"
}

reset_stack() {
  compose --profile benchbase down -v --remove-orphans
}

normalize_benchmark() {
  case "${BENCHMARK}" in
    tpc-h) BENCHMARK="tpch" ;;
    tpc-ds) BENCHMARK="tpcds" ;;
  esac
}

join_by_comma() {
  local IFS=,
  echo "$*"
}

read_cluster_nodes() {
  local props="${REPO_ROOT}/src/main/resources/arrowflight.properties"
  local nodes
  nodes="${BENCHBASE_CLUSTER_NODES:-}"
  if [[ -z "${nodes}" ]]; then
    nodes="$(awk -F= '/^[[:space:]]*numServers[[:space:]]*=/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "${props}")"
  fi
  nodes="${nodes:-3}"

  if [[ ! "${nodes}" =~ ^[0-9]+$ ]] || (( nodes < 1 || nodes > 10 )); then
    echo "Bad cluster node count=${nodes}. Supported: 1-10." >&2
    exit 2
  fi

  echo "${nodes}"
}

configure_cluster() {
  local nodes
  nodes="$(read_cluster_nodes)"

  local hosts=()
  local servers=()
  local data_dirs=()
  FLIGHT_SERVER_SERVICES=()

  for index in $(seq 1 "${nodes}"); do
    hosts+=("flight-server-${index}")
    servers+=("flight-server-${index}:32010")
    data_dirs+=("/server-data/server-node-${index}")
    FLIGHT_SERVER_SERVICES+=("server-node-${index}")
  done

  export FLIGHT_HOSTS
  export FLIGHT_SERVERS
  export FLIGHT_SERVER_DATA_DIRS
  export FLIGHT_SOURCE_HOST="flight-server-1"
  export FLIGHT_SOURCE_PORT="32010"
  export BENCHMARK_DATASET="${BENCHMARK}"
  export BENCHMARK_SCHEMA="${BENCHMARK}"
  export BENCHMARK_SOURCE_SCHEMA="${BENCHMARK}"
  export BENCHMARK_QUERY_SET="${QUERY_SET}"
  export BENCHMARK_SCALE_FACTOR="${BENCHMARK_SCALE_FACTOR:-0.01}"
  export HDFS_DATA_DIR="${HDFS_DATA_DIR:-hdfs://hdfs-namenode:8020/bench}"
  export HDFS_BENCHMARK_PATH="${HDFS_BENCHMARK_PATH:-/bench}"
  export HDFS_BLOCK_SIZE_BYTES="${HDFS_BLOCK_SIZE_BYTES:-1073741824}"
  export DIRECT_PARQUET_DIR="${HDFS_DATA_DIR}"
  export DIRECT_PARQUET_PARTITIONS="${DIRECT_PARQUET_PARTITIONS:-${nodes}}"

  FLIGHT_HOSTS="$(join_by_comma "${hosts[@]}")"
  FLIGHT_SERVERS="$(join_by_comma "${servers[@]}")"
  FLIGHT_SERVER_DATA_DIRS="$(join_by_comma "${data_dirs[@]}")"

  echo "clusterNodes=${nodes}"
  echo "FLIGHT_HOSTS=${FLIGHT_HOSTS}"
  echo "HDFS_DATA_DIR=${HDFS_DATA_DIR}"
  echo "HDFS_BLOCK_SIZE_BYTES=${HDFS_BLOCK_SIZE_BYTES}"
  echo "BENCHMARK_SCALE_FACTOR=${BENCHMARK_SCALE_FACTOR}"
  echo "SPARK_SQL_ANSI_ENABLED=${SPARK_SQL_ANSI_ENABLED}"
  echo "FLIGHT_BATCH_SIZE=${FLIGHT_BATCH_SIZE}"
  echo "FLIGHT_DUCKDB_THREADS=${FLIGHT_DUCKDB_THREADS:-properties default}"
  echo "FLIGHT_TIMING_LOG_LEVEL=${FLIGHT_TIMING_LOG_LEVEL:-inherits FLIGHT_LOG_LEVEL}"
  if [[ "${SPARK_SQL_ANSI_ENABLED,,}" != "true" ]]; then
    echo "WARNING: ANSI=false prevents Spark 3.5 from pushing TPC-H Q1 decimal aggregates" >&2
    echo "to Flight; use SPARK_SQL_ANSI_ENABLED=true for representative Flight performance." >&2
  fi
}

prepare_results_dir() {
  if [[ -z "${RESULTS_RUN_ID}" ]]; then
    local query_label="${QUERY_SET:-all}"
    query_label="${query_label,,}"
    query_label="${query_label//[^a-z0-9,]/-}"
    RESULTS_RUN_ID="${BENCHMARK}-${MODE}-${query_label}-$(date +%Y%m%d-%H%M%S)"
  fi

  RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_RUN_ID}"
  RESULTS_IN_CONTAINER="/benchbase/results/${RESULTS_RUN_ID}"
  mkdir -p "${RESULTS_DIR}"
  chmod a+rwx "${RESULTS_DIR}" 2>/dev/null || true
  echo "Results directory: ${RESULTS_DIR}"
}

prepare_metadata_output() {
  export BENCHMARK_METADATA_OUT="${RESULTS_IN_CONTAINER}/benchmark-metadata.json"
}

init_machine_result() {
  local results_root="$1"
  local nodes
  local -a measurement_args=()
  nodes="$(read_cluster_nodes)"
  if [[ -n "${BENCHBASE_TIME_SECONDS}" ]]; then
    measurement_args=(--measurement-seconds "${BENCHBASE_TIME_SECONDS}")
  fi

  run_python "${SCRIPT_DIR}/benchmark-result-schema.py" init \
    --results "${results_root}" \
    --repo-root "${REPO_ROOT}" \
    --benchmark "${BENCHMARK}" \
    --mode "${MODE}" \
    --query-set "${QUERY_SET:-all}" \
    --scale-factor "${BENCHMARK_SCALE_FACTOR}" \
    --warmup-seconds "${BENCHBASE_WARMUP_SECONDS:-0}" \
    "${measurement_args[@]}" \
    --cache-policy "${BENCHBASE_CACHE_POLICY}" \
    --repetitions "${BENCHBASE_QUERY_REPETITIONS}" \
    --paired-observations "${BENCHBASE_PAIRED_OBSERVATIONS}" \
    --terminals "${BENCHBASE_TERMINALS:-1}" \
    --rate "${BENCHBASE_RATE}" \
    --engine-order "${BENCHBASE_COMPARE_ORDER}" \
    --cluster-nodes "${nodes}" \
    --flight-hosts "${FLIGHT_HOSTS}" \
    --flight-servers "${FLIGHT_SERVERS}" \
    --host-resources "${BENCHBASE_HOST_RESOURCES:-not-recorded}" \
    --spark-master-url "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --spark-sql-ansi-enabled "${SPARK_SQL_ANSI_ENABLED}" \
    --spark-thrift-server "spark-thrift-server:10000" \
    --direct-parquet-partitions "${DIRECT_PARQUET_PARTITIONS}" \
    --hdfs-data-dir "${HDFS_DATA_DIR}" \
    --hdfs-benchmark-path "${HDFS_BENCHMARK_PATH}" \
    --hdfs-block-size-bytes "${HDFS_BLOCK_SIZE_BYTES}" \
    --hdfs-replication "1" \
    --java-opts "${JAVA_OPTS:--Xmx2g}" \
    --flight-source-host "${FLIGHT_SOURCE_HOST}" \
    --flight-source-port "${FLIGHT_SOURCE_PORT}" \
    --flight-batch-size "${FLIGHT_BATCH_SIZE}" \
    --flight-duckdb-threads "${FLIGHT_DUCKDB_THREADS:-properties-default}" \
    --flight-timing-log-level "${FLIGHT_TIMING_LOG_LEVEL:-inherited}" \
    --flight-log-level "${FLIGHT_LOG_LEVEL:-INFO}" >/dev/null
}

build_machine_result() {
  local results_root="$1"
  run_python "${SCRIPT_DIR}/benchmark-result-schema.py" build \
    --results "${results_root}"
  run_python "${SCRIPT_DIR}/benchmark-result-schema.py" validate \
    "${results_root}/benchmark-result.json" >/dev/null
}

start_paired_observation() {
  local results_root="$1"
  local observation_index="$2"
  run_python "${SCRIPT_DIR}/benchmark-result-schema.py" observation-start \
    --results "${results_root}" \
    --observation-index "${observation_index}" >/dev/null
}

finish_paired_observation() {
  local results_root="$1"
  local observation_index="$2"
  local flight_status="$3"
  local direct_status="$4"
  run_python "${SCRIPT_DIR}/benchmark-result-schema.py" observation-finish \
    --results "${results_root}" \
    --observation-index "${observation_index}" \
    --flight-exit-code "${flight_status}" \
    --direct-exit-code "${direct_status}" >/dev/null
}

cleanup_generated_config() {
  local file
  for file in "${GENERATED_CONFIGS[@]}"; do
    [[ -f "${file}" ]] && rm -f "${file}"
  done
}

tpch_query_weights() {
  local normalized="${QUERY_SET,,}"
  normalized="${normalized// /}"

  local weights=()
  local selected=()
  for _ in $(seq 1 22); do
    weights+=(0)
  done

  if [[ "${normalized}" == "all" ]]; then
    normalized="$(seq -s, 1 22)"
  fi

  IFS=',' read -ra tokens <<< "${normalized}"
  for token in "${tokens[@]}"; do
    token="${token#q}"
    if [[ -z "${token}" || ! "${token}" =~ ^[0-9]+$ ]]; then
      echo "Bad TPC-H query selector: ${QUERY_SET}" >&2
      exit 2
    fi

    local query_id=$((10#${token}))
    if (( query_id < 1 || query_id > 22 )); then
      echo "TPC-H query must be between q1 and q22: q${query_id}" >&2
      exit 2
    fi

    selected+=("${query_id}")
  done

  local count="${#selected[@]}"
  local base=$((100 / count))
  local rest=$((100 - base * count))
  local index=0
  for query_id in "${selected[@]}"; do
    local value="${base}"
    if (( index < rest )); then
      value=$((value + 1))
    fi
    weights[$((query_id - 1))]="${value}"
    index=$((index + 1))
  done

  join_by_comma "${weights[@]}"
}

tpcds_query_weights() {
  local normalized="${QUERY_SET,,}"
  normalized="${normalized// /}"

  local weights=()
  local selected=()
  for _ in $(seq 1 99); do
    weights+=(0)
  done

  IFS=',' read -ra tokens <<< "${normalized}"
  for token in "${tokens[@]}"; do
    token="${token#q}"
    if [[ -z "${token}" || ! "${token}" =~ ^[0-9]+$ ]]; then
      echo "Bad TPC-DS query selector: ${QUERY_SET}" >&2
      exit 2
    fi

    local query_id=$((10#${token}))
    if (( query_id < 1 || query_id > 99 )); then
      echo "TPC-DS query must be between q1 and q99: q${query_id}" >&2
      exit 2
    fi

    selected+=("${query_id}")
  done

  local count="${#selected[@]}"
  local base=$((100 / count))
  local rest=$((100 - base * count))
  local index=0
  for query_id in "${selected[@]}"; do
    local value="${base}"
    if (( index < rest )); then
      value=$((value + 1))
    fi
    weights[$((query_id - 1))]="${value}"
    index=$((index + 1))
  done

  join_by_comma "${weights[@]}"
}

prepare_execute_config() {
  EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/${BENCHMARK}.xml"
  local db_schema="${BENCHBASE_DB_SCHEMA:-${BENCHMARK}}"

  if [[ ! "${BENCHBASE_QUERY_REPETITIONS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCHBASE_QUERY_REPETITIONS must be a positive integer: ${BENCHBASE_QUERY_REPETITIONS}" >&2
    exit 2
  fi
  if [[ "${BENCHBASE_QUERY_REPETITIONS}" != "1" && -n "${BENCHBASE_TIME_SECONDS}" ]]; then
    echo "BENCHBASE_QUERY_REPETITIONS cannot be combined with BENCHBASE_TIME_SECONDS." >&2
    exit 2
  fi
  if [[ ! "${BENCHBASE_WARMUP_SECONDS:-0}" =~ ^[0-9]+$ ]]; then
    echo "BENCHBASE_WARMUP_SECONDS must be a non-negative integer: ${BENCHBASE_WARMUP_SECONDS}" >&2
    exit 2
  fi
  if [[ -n "${BENCHBASE_TIME_SECONDS}" && ! "${BENCHBASE_TIME_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCHBASE_TIME_SECONDS must be a positive integer: ${BENCHBASE_TIME_SECONDS}" >&2
    exit 2
  fi

  if [[ -z "${QUERY_SET}" && -z "${BENCHBASE_TIME_SECONDS}" && -z "${BENCHBASE_WARMUP_SECONDS}" && -z "${BENCHBASE_TERMINALS}" && "${BENCHBASE_QUERY_REPETITIONS}" == "1" && "${db_schema}" == "${BENCHMARK}" && "${BENCHMARK_SCALE_FACTOR}" == "0.01" ]]; then
    return
  fi

  local base_config="${CONFIG_DIR}/${BENCHMARK}.xml"
  local safe_schema="${db_schema,,}"
  safe_schema="${safe_schema//[^a-z0-9_]/-}"
  local safe_selector="${QUERY_SET:-all}"
  safe_selector="${safe_selector,,}"
  safe_selector="${safe_selector//[^a-z0-9,]/-}"
  GENERATED_CONFIG_LOCAL="${CONFIG_DIR}/.${BENCHMARK}-${safe_schema}-${safe_selector}-${BENCHBASE_TIME_SECONDS:-serial}-r${BENCHBASE_QUERY_REPETITIONS}.xml"
  cp "${base_config}" "${GENERATED_CONFIG_LOCAL}"
  GENERATED_CONFIGS+=("${GENERATED_CONFIG_LOCAL}")

  sed -i "s#<scalefactor>.*</scalefactor>#  <scalefactor>${BENCHMARK_SCALE_FACTOR}</scalefactor>#" "${GENERATED_CONFIG_LOCAL}"

  if [[ -n "${QUERY_SET}" ]]; then
    local weights
    if [[ "${BENCHMARK}" == "tpcds" ]]; then
      weights="$(tpcds_query_weights)"
    else
      weights="$(tpch_query_weights)"
    fi
    sed -i "s#<weights>.*</weights>#      <weights>${weights}</weights>#" "${GENERATED_CONFIG_LOCAL}"
  fi

  if [[ "${db_schema}" != "${BENCHMARK}" ]]; then
    sed -i "s#jdbc:hiveexec:hive2://spark-thrift-server:10000/[^<]*#jdbc:hiveexec:hive2://spark-thrift-server:10000/${db_schema}#" "${GENERATED_CONFIG_LOCAL}"
  fi

  if [[ -n "${BENCHBASE_TERMINALS}" ]]; then
    sed -i "s#<terminals>.*</terminals>#  <terminals>${BENCHBASE_TERMINALS}</terminals>#" "${GENERATED_CONFIG_LOCAL}"
  fi

  if [[ -n "${BENCHBASE_TIME_SECONDS}" ]]; then
    sed -i "s#<serial>.*</serial>#      <serial>false</serial>#" "${GENERATED_CONFIG_LOCAL}"
    sed -i "s#<rate>.*</rate>#      <rate>${BENCHBASE_RATE}</rate>#" "${GENERATED_CONFIG_LOCAL}"
    sed -i "/<serial>false<\/serial>/a\\      <time>${BENCHBASE_TIME_SECONDS}</time>\\n      <warmup>${BENCHBASE_WARMUP_SECONDS:-0}</warmup>" "${GENERATED_CONFIG_LOCAL}"
    BENCHBASE_TIMED_QUERY_COUNT="$(
      run_python "${SCRIPT_DIR}/split-timed-work-phases.py" \
        --config "${GENERATED_CONFIG_LOCAL}"
    )"
    if [[ ! "${BENCHBASE_TIMED_QUERY_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
      echo "Could not determine the number of timed query phases." >&2
      exit 2
    fi
  elif [[ "${BENCHBASE_QUERY_REPETITIONS}" != "1" ]]; then
    run_python "${SCRIPT_DIR}/repeat-work-phases.py" \
      --config "${GENERATED_CONFIG_LOCAL}" \
      --repetitions "${BENCHBASE_QUERY_REPETITIONS}"
  fi

  EXEC_CONFIG_IN_CONTAINER="/benchbase/config/custom/$(basename "${GENERATED_CONFIG_LOCAL}")"
  trap cleanup_generated_config EXIT
}

wait_service_healthy() {
  local service="$1"
  local timeout_seconds="${2:-180}"
  local deadline=$((SECONDS + timeout_seconds))
  local container_id=""
  local status=""

  while (( SECONDS < deadline )); do
    container_id="$(compose --profile benchbase ps -q "${service}" 2>/dev/null || true)"
    if [[ -n "${container_id}" ]]; then
      status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}" 2>/dev/null || true)"
      if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
        echo "${service} ready"
        return
      fi
      if [[ "${status}" == "exited" || "${status}" == "dead" ]]; then
        break
      fi
    fi
    sleep 2
  done

  echo "${service} did not become ready. Last status: ${status:-unknown}." >&2
  compose --profile benchbase logs --tail=100 "${service}" >&2 || true
  exit 1
}

start_storage_cluster() {
  echo "Building shared ArrowFlight image once"
  compose build hdfs-namenode
  compose up --no-build -d hdfs-namenode spark-master "${FLIGHT_SERVER_SERVICES[@]}"
  wait_service_healthy hdfs-namenode 180
  start_observability
}

start_observability() {
  if [[ "${BENCHMARK_OBSERVABILITY,,}" != "true" ]]; then
    return
  fi
  compose --profile observability up -d prometheus grafana node-exporter cadvisor
  echo "Grafana: http://localhost:${GRAFANA_PORT:-3000}/d/arrowflight-benchmark"
}

start_thrift() {
  compose --profile benchbase up --no-build --force-recreate -d spark-thrift-server
  wait_service_healthy spark-thrift-server 180
}

wait_flight_servers() {
  local service
  for service in "${FLIGHT_SERVER_SERVICES[@]}"; do
    wait_service_healthy "${service}" 180
  done
}

register_flight_tables() {
  compose --profile benchbase run --rm --entrypoint bash spark-benchmark-publisher \
    -c "rm -rf /spark-warehouse/metastore_db && echo 'Cleaned stale Derby metastore'"
  compose --profile benchbase run --use-aliases --rm \
    -e BENCHMARK_PUBLISH_MODE=flight \
    -e BENCHMARK_SOURCE_SCHEMA="${BENCHMARK}" \
    -e SPARK_PUBLISH_SCHEMA="${BENCHMARK}" \
    spark-benchmark-publisher
}

register_compare_tables() {
  compose --profile benchbase run --rm --entrypoint bash spark-benchmark-publisher \
    -c "rm -rf /spark-warehouse/metastore_db && echo 'Cleaned stale Derby metastore'"
  compose --profile benchbase run --use-aliases --rm \
    -e BENCHMARK_PUBLISH_MODE=both \
    -e BENCHMARK_SOURCE_SCHEMA="${BENCHMARK}" \
    -e FLIGHT_PUBLISH_SCHEMA="${BENCHMARK}_flight" \
    -e DIRECT_PUBLISH_SCHEMA="${BENCHMARK}_direct" \
    spark-benchmark-publisher
}

wait_hdfs_datanodes() {
  local expected="${#FLIGHT_SERVER_SERVICES[@]}"
  local deadline=$((SECONDS + 180))
  local report=""
  while (( SECONDS < deadline )); do
    report="$(compose exec -T hdfs-namenode hdfs dfsadmin -report 2>/dev/null || true)"
    if grep -q "Live datanodes (${expected})" <<< "${report}"; then
      echo "HDFS DataNodes ready: ${expected}"
      return
    fi
    sleep 2
  done
  echo "HDFS did not register ${expected} DataNodes." >&2
  compose logs --tail=100 hdfs-namenode "${FLIGHT_SERVER_SERVICES[@]}" >&2 || true
  exit 1
}

generate_parquet_data() {
  if [[ "${BENCHMARK_REBUILD_GENERATOR,,}" != "true" ]] \
      && docker image inspect "${BENCHMARK_GENERATOR_IMAGE}" >/dev/null 2>&1; then
    echo "Using cached benchmark generator image: ${BENCHMARK_GENERATOR_IMAGE}"
  else
    if [[ ! "${BENCHMARK_GENERATOR_BUILD_RETRIES}" =~ ^[1-9][0-9]*$ ]]; then
      echo "BENCHMARK_GENERATOR_BUILD_RETRIES must be a positive integer." >&2
      exit 2
    fi

    local attempt
    for ((attempt = 1; attempt <= BENCHMARK_GENERATOR_BUILD_RETRIES; attempt++)); do
      echo "Building benchmark generator image (attempt ${attempt}/${BENCHMARK_GENERATOR_BUILD_RETRIES})"
      if compose --profile benchbase build duckdb-benchmark-generator; then
        break
      fi
      if (( attempt == BENCHMARK_GENERATOR_BUILD_RETRIES )); then
        echo "Failed to build ${BENCHMARK_GENERATOR_IMAGE} after ${attempt} attempts." >&2
        return 1
      fi
      sleep $((attempt * 5))
    done
  fi
  compose --profile benchbase run --rm duckdb-benchmark-generator
}

upload_parquet_to_hdfs() {
  compose exec -T hdfs-namenode hdfs dfs -mkdir -p "${HDFS_BENCHMARK_PATH}"
  compose exec -T hdfs-namenode hdfs dfs -rm -r -f \
    "${HDFS_BENCHMARK_PATH}/${BENCHMARK}" >/dev/null 2>&1 || true

  local service
  for service in "${FLIGHT_SERVER_SERVICES[@]}"; do
    echo "Uploading ${service} Parquet shard(s) to HDFS"
    compose exec -T "${service}" bash -lc '
      set -euo pipefail
      schema="$1"
      hdfs_root="$2"
      shopt -s nullglob
      table_dirs=("/staging/${schema}"/*)
      if (( ${#table_dirs[@]} == 0 )); then
        echo "No staged tables under /staging/${schema}" >&2
        exit 1
      fi
      for table_dir in "${table_dirs[@]}"; do
        [[ -d "${table_dir}" ]] || continue
        table="$(basename "${table_dir}")"
        files=("${table_dir}"/*.parquet)
        if (( ${#files[@]} == 0 )); then
          echo "No Parquet files under ${table_dir}" >&2
          exit 1
        fi
        hdfs dfs -mkdir -p "${hdfs_root}/${schema}/${table}"
        hdfs dfs -put -f "${files[@]}" "${hdfs_root}/${schema}/${table}/"
      done
    ' _ "${BENCHMARK}" "${HDFS_BENCHMARK_PATH}"
  done

  compose exec -T hdfs-namenode hdfs dfs -setrep -w 1 \
    "${HDFS_BENCHMARK_PATH}/${BENCHMARK}" >/dev/null
  compose exec -T hdfs-namenode hdfs fsck \
    "${HDFS_BENCHMARK_PATH}/${BENCHMARK}"
  compose exec -T hdfs-namenode hdfs dfs -touchz \
    "${HDFS_BENCHMARK_PATH}/_READY"
}

verify_spark_schema() {
  local schema="$1"
  local label="${2:-${schema}}"
  local table
  case "${BENCHMARK}" in
    tpch) table="nation" ;;
    tpcds) table="date_dim" ;;
    *) table="" ;;
  esac

  if [[ -z "${table}" ]]; then
    return
  fi

  echo "Verifying Spark Thrift can read ${schema}.${table} (${label})"
  compose --profile benchbase exec -T spark-thrift-server bash -lc \
    "printf '%s\n' 'SELECT COUNT(*) AS row_count FROM ${schema}.${table};' > /tmp/verify-${schema}.sql && /opt/spark/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/${schema}' -n benchbase -f /tmp/verify-${schema}.sql"
}

verify_spark_flight() {
  verify_spark_schema "${BENCHMARK}" "Flight"
}

verify_compare_tables() {
  verify_spark_schema "${BENCHMARK}_flight" "FlightSource"
  verify_spark_schema "${BENCHMARK}_direct" "direct Parquet"
}

fail_on_benchbase_sql_errors() {
  local log_file="$1"
  if awk '
    /Unexpected SQL Errors:/ { section = 1; next }
    /Unknown Status Transactions:/ { section = 0 }
    section && /\[[[:space:]]*[1-9][0-9]*\]/ { bad = 1 }
    END { exit bad ? 0 : 1 }
  ' "${log_file}"; then
    echo "BenchBase reported unexpected SQL errors. See ${log_file}" >&2
    return 1
  fi
}

is_flight_schema() {
  local db_schema="$1"
  [[ "${db_schema}" == "${BENCHMARK}" || "${db_schema}" == "${BENCHMARK}_flight" ]]
}

reset_execution_path_events() {
  local service
  for service in "${FLIGHT_SERVER_SERVICES[@]}"; do
    compose --profile benchbase exec -T "${service}" \
      rm -f "${EXECUTION_PATH_LOG_IN_CONTAINER}"
  done
}

capture_execution_path_events() {
  local service
  local output_dir="${RESULTS_DIR}/execution-paths"
  mkdir -p "${output_dir}"
  for service in "${FLIGHT_SERVER_SERVICES[@]}"; do
    local output="${output_dir}/${service}.jsonl"
    compose --profile benchbase exec -T "${service}" sh -c \
      "if [ -f '${EXECUTION_PATH_LOG_IN_CONTAINER}' ]; then cat '${EXECUTION_PATH_LOG_IN_CONTAINER}'; fi" \
      > "${output}"
    if [[ ! -s "${output}" ]]; then
      rm -f "${output}"
    fi
  done
}

record_benchmark_failure() {
  local reason="$1"
  local exit_code="$2"
  printf '{\n  "reason": "%s",\n  "exit_code": %s\n}\n' \
    "${reason}" "${exit_code}" > "${RESULTS_DIR}/benchmark-failure.json"
}

benchbase_progress() {
  local db_schema="$1"
  local interval_seconds="${BENCHBASE_PROGRESS_INTERVAL_SECONDS:-30}"
  local elapsed_seconds=0
  local warmup_seconds="${BENCHBASE_WARMUP_SECONDS:-0}"
  local timed_query_count="${BENCHBASE_TIMED_QUERY_COUNT:-1}"
  local total_seconds=$((warmup_seconds + BENCHBASE_TIME_SECONDS * timed_query_count))

  if [[ ! "${interval_seconds}" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCHBASE_PROGRESS_INTERVAL_SECONDS must be a positive integer: ${interval_seconds}" >&2
    return 2
  fi
  if [[ ! "${timed_query_count}" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCHBASE_TIMED_QUERY_COUNT must be a positive integer: ${timed_query_count}" >&2
    return 2
  fi

  while sleep "${interval_seconds}"; do
    elapsed_seconds=$((elapsed_seconds + interval_seconds))
    if (( elapsed_seconds < warmup_seconds )); then
      echo "[BenchBase] ${db_schema}: initial warmup running, $((warmup_seconds - elapsed_seconds))s remaining"
    elif (( elapsed_seconds < total_seconds )); then
      local measured_elapsed=$((elapsed_seconds - warmup_seconds))
      local phase_index=$((measured_elapsed / BENCHBASE_TIME_SECONDS + 1))
      local phase_elapsed=$((measured_elapsed % BENCHBASE_TIME_SECONDS))
      echo "[BenchBase] ${db_schema}: query phase ${phase_index}/${timed_query_count}; measurement running, $((BENCHBASE_TIME_SECONDS - phase_elapsed))s remaining"
    else
      echo "[BenchBase] ${db_schema}: all configured measurement windows ended; waiting for the current query before exit"
    fi
  done
}

prune_inactive_query_csv() {
  if [[ -z "${QUERY_SET}" ]]; then
    return
  fi

  local normalized="${QUERY_SET,,}"
  normalized="${normalized// /}"
  if [[ "${normalized}" == "all" ]]; then
    return
  fi

  local -A active_queries=()
  local token
  IFS=',' read -ra tokens <<< "${normalized}"
  for token in "${tokens[@]}"; do
    token="${token#q}"
    if [[ "${token}" =~ ^[0-9]+$ ]]; then
      active_queries[$((10#${token}))]=1
    fi
  done

  local file name query_id pruned=0
  shopt -s nullglob
  for file in "${RESULTS_DIR}"/*.results.Q*.csv; do
    name="$(basename "${file}")"
    query_id="${name##*.results.Q}"
    query_id="${query_id%.csv}"
    if [[ "${query_id}" =~ ^[0-9]+$ && -z "${active_queries[$((10#${query_id}))]+x}" ]]; then
      rm -f "${file}"
      pruned=$((pruned + 1))
    fi
  done
  shopt -u nullglob

  if (( pruned > 0 )); then
    echo "Removed ${pruned} inactive per-query CSV file(s) from ${RESULTS_DIR}"
  fi
}

metadata_file_for_results() {
  if [[ -f "${RESULTS_DIR}/benchmark-metadata.json" ]]; then
    echo "${RESULTS_DIR}/benchmark-metadata.json"
  elif [[ -f "$(dirname "${RESULTS_DIR}")/benchmark-metadata.json" ]]; then
    echo "$(dirname "${RESULTS_DIR}")/benchmark-metadata.json"
  elif [[ -f "$(dirname "$(dirname "$(dirname "${RESULTS_DIR}")")")/benchmark-metadata.json" ]]; then
    echo "$(dirname "$(dirname "$(dirname "${RESULTS_DIR}")")")/benchmark-metadata.json"
  else
    echo ""
  fi
}

capture_query_results() {
  local metadata_file
  metadata_file="$(metadata_file_for_results)"
  if [[ -z "${metadata_file}" ]]; then
    return
  fi

  if [[ ! "${BENCHBASE_CAPTURE_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]]; then
    echo "BENCHBASE_CAPTURE_TIMEOUT_SECONDS must be a non-negative integer: ${BENCHBASE_CAPTURE_TIMEOUT_SECONDS}" >&2
    return 2
  fi

  run_python "${SCRIPT_DIR}/capture-query-results.py" \
    --metadata "${metadata_file}" \
    --results "${RESULTS_DIR}" \
    --queries "${QUERY_SET}" >/dev/null

  local db_schema="${BENCHBASE_DB_SCHEMA:-${BENCHMARK}}"
  local sql_file name sql_in_container out_in_container out_local capture_status timeout_prefix
  local plan_in_container plan_local
  shopt -s nullglob
  for sql_file in "${RESULTS_DIR}"/query-q*.sql; do
    name="$(basename "${sql_file}")"
    sql_in_container="${RESULTS_IN_CONTAINER}/${name}"
    out_in_container="${RESULTS_IN_CONTAINER}/${name%.sql}.actual.csv"
    out_local="${RESULTS_DIR}/${name%.sql}.actual.csv"
    plan_in_container="${RESULTS_IN_CONTAINER}/${name%.sql}.plan.txt"
    plan_local="${RESULTS_DIR}/${name%.sql}.plan.txt"
    timeout_prefix=""
    if (( BENCHBASE_CAPTURE_TIMEOUT_SECONDS > 0 )); then
      timeout_prefix="timeout --foreground --signal=TERM --kill-after=10s '${BENCHBASE_CAPTURE_TIMEOUT_SECONDS}s' "
      echo "[BenchBase] Capturing ${name} in schema ${db_schema}; timeout=${BENCHBASE_CAPTURE_TIMEOUT_SECONDS}s"
    else
      echo "[BenchBase] Capturing ${name} in schema ${db_schema}; timeout=disabled"
    fi
    if ! compose --profile benchbase exec -T spark-thrift-server bash -lc \
      "sed '1s/^/EXPLAIN FORMATTED /' '${sql_in_container}' > /tmp/benchmark-plan.sql && ${timeout_prefix}/opt/spark/bin/beeline --silent=true --showHeader=false --outputformat=csv2 -u 'jdbc:hive2://127.0.0.1:10000/${db_schema}' -n benchbase -f /tmp/benchmark-plan.sql > '${plan_in_container}'"; then
      rm -f "${plan_local}"
      echo "Could not capture formatted physical plan for ${name} in schema ${db_schema}" >&2
    fi
    if compose --profile benchbase exec -T spark-thrift-server bash -lc \
      "${timeout_prefix}/opt/spark/bin/beeline --silent=true --showHeader=true --outputformat=csv2 -u 'jdbc:hive2://127.0.0.1:10000/${db_schema}' -n benchbase -f '${sql_in_container}' > '${out_in_container}'"; then
      echo "[BenchBase] Captured ${name} in schema ${db_schema}"
    else
      capture_status="$?"
      rm -f "${out_local}"
      if (( capture_status == 124 || capture_status == 137 )); then
        echo "Reference capture timed out for ${name} in schema ${db_schema}; report will mark it not captured" >&2
      else
        echo "Could not capture actual result for ${name} in schema ${db_schema} (exit ${capture_status})" >&2
      fi
    fi
  done
  shopt -u nullglob
}

build_html_report() {
  run_python "${SCRIPT_DIR}/visualize-results.py" --results "${RESULTS_DIR}"
  if [[ -z "${COMPARE_PARENT_RUN_ID}" ]]; then
    build_pages_site
  fi
}

build_compare_html_report() {
  run_python "${SCRIPT_DIR}/visualize-results.py" --compare --results "${RESULTS_DIR}"
  build_pages_site
}

build_pages_site() {
  if [[ "${BENCHBASE_UPDATE_PAGES}" != "true" ]]; then
    return
  fi
  run_python "${SCRIPT_DIR}/build-benchmark-report.py" \
    --results "${RESULTS_ROOT}" >/dev/null
  run_python "${SCRIPT_DIR}/build-pages-site.py" --results "${RESULTS_ROOT}" --out "${PAGES_DIR}"
  run_python "${SCRIPT_DIR}/validate-benchmark-publication.py" \
    --pages "${PAGES_DIR}" >/dev/null
}

ensure_benchbase_image() {
  if [[ "${BENCHBASE_IMAGE_READY}" == "true" ]]; then
    return
  fi
  compose --profile benchbase build benchbase-spark
  BENCHBASE_IMAGE_READY=true
}

benchbase_execute() {
  prepare_execute_config
  prepare_results_dir
  ensure_benchbase_image

  local db_schema="${BENCHBASE_DB_SCHEMA:-${BENCHMARK}}"
  local log_file="${RESULTS_DIR}/last-${BENCHMARK}-${db_schema}.log"
  local progress_pid=""
  if is_flight_schema "${db_schema}"; then
    reset_execution_path_events
  fi
  echo "[BenchBase] Starting schema=${db_schema}, initialWarmup=${BENCHBASE_WARMUP_SECONDS:-0}s, measurement=${BENCHBASE_TIME_SECONDS:-serial}s/query, timedQueries=${BENCHBASE_TIMED_QUERY_COUNT}, repetitions=${BENCHBASE_QUERY_REPETITIONS}, terminals=${BENCHBASE_TERMINALS:-config default}"
  if [[ -n "${BENCHBASE_TIME_SECONDS}" ]]; then
    benchbase_progress "${db_schema}" &
    progress_pid="$!"
  fi

  set +e
  compose --profile benchbase run --rm benchbase-spark \
    -b "${BENCHMARK}" \
    -c "${EXEC_CONFIG_IN_CONTAINER}" \
    --create=false \
    --load=false \
    --execute=true \
    -d "${RESULTS_IN_CONTAINER}" 2>&1 | tee "${log_file}"
  local benchbase_status="${PIPESTATUS[0]}"
  if [[ -n "${progress_pid}" ]]; then
    kill "${progress_pid}" 2>/dev/null
    wait "${progress_pid}" 2>/dev/null
  fi
  set -e

  if is_flight_schema "${db_schema}"; then
    capture_execution_path_events
  fi

  if (( benchbase_status != 0 )); then
    record_benchmark_failure "benchbase-exit-${benchbase_status}" "${benchbase_status}"
    return "${benchbase_status}"
  fi
  echo "[BenchBase] Completed schema=${db_schema}"
  if ! fail_on_benchbase_sql_errors "${log_file}"; then
    record_benchmark_failure "unexpected-sql-errors" 1
    return 1
  fi
  prune_inactive_query_csv
  capture_query_results
  build_html_report
}

prepare_stack() {
  start_storage_cluster
  wait_hdfs_datanodes
  generate_parquet_data
  upload_parquet_to_hdfs
  wait_flight_servers
  register_flight_tables
  start_thrift
  verify_spark_flight
}

prepare_compare_stack() {
  start_storage_cluster
  wait_hdfs_datanodes
  generate_parquet_data
  upload_parquet_to_hdfs
  wait_flight_servers
  register_compare_tables
  start_thrift
  verify_compare_tables
}

compare_execute() {
  local parent_run_id="${COMPARE_PARENT_RUN_ID}"
  local results_root="${RESULTS_ROOT}/${parent_run_id}"
  local observation_index first_path second_path path path_status
  local flight_status direct_status
  local comparison_status=0
  local -a scheduled_observations=()

  if [[ -n "${BENCHBASE_TIME_SECONDS}" ]]; then
    echo "[BenchBase] Paired observations=${BENCHBASE_PAIRED_OBSERVATIONS}; starting order=${BENCHBASE_COMPARE_ORDER}; cache policy=${BENCHBASE_CACHE_POLICY}; every selected query gets ${BENCHBASE_TIME_SECONDS}s measurement per path after ${BENCHBASE_WARMUP_SECONDS:-0}s warmup."
  else
    echo "[BenchBase] Paired observations=${BENCHBASE_PAIRED_OBSERVATIONS}; starting order=${BENCHBASE_COMPARE_ORDER}; cache policy=${BENCHBASE_CACHE_POLICY}; selected queries run ${BENCHBASE_QUERY_REPETITIONS} measured time(s) per path in serial mode."
  fi

  mapfile -t scheduled_observations < <(
    run_python "${SCRIPT_DIR}/benchmark-result-schema.py" schedule \
      --paired-observations "${BENCHBASE_PAIRED_OBSERVATIONS}" \
      --engine-order "${BENCHBASE_COMPARE_ORDER}"
  )
  for scheduled in "${scheduled_observations[@]}"; do
    IFS=$'\t' read -r observation_index first_path second_path <<< "${scheduled}"
    local observation_id
    observation_id="$(printf 'observation-%03d' "${observation_index}")"
    flight_status=0
    direct_status=0
    start_paired_observation "${results_root}" "${observation_index}"
    echo "[BenchBase] Observation ${observation_index}/${BENCHBASE_PAIRED_OBSERVATIONS}; order=${first_path}-${second_path}"
    for path in "${first_path}" "${second_path}"; do
      if run_compare_path "${parent_run_id}" "${observation_id}" "${path}"; then
        path_status=0
      else
        path_status="$?"
        comparison_status=1
      fi
      if [[ "${path}" == "flight" ]]; then
        flight_status="${path_status}"
      else
        direct_status="${path_status}"
      fi
    done
    finish_paired_observation \
      "${results_root}" \
      "${observation_index}" \
      "${flight_status}" \
      "${direct_status}"
  done

  RESULTS_RUN_ID="${parent_run_id}"
  RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_RUN_ID}"
  RESULTS_IN_CONTAINER="/benchbase/results/${RESULTS_RUN_ID}"
  build_machine_result "${RESULTS_DIR}"
  build_compare_html_report

  echo "Compare results written under ${RESULTS_ROOT}/${parent_run_id}"
  return "${comparison_status}"
}

run_compare_path() {
  local parent_run_id="$1"
  local observation_id="$2"
  local path="$3"

  BENCHBASE_DB_SCHEMA="${BENCHMARK}_${path}"
  RESULTS_RUN_ID="${parent_run_id}/observations/${observation_id}/${path}"
  benchbase_execute
}

init_compare_run() {
  local query_label="${QUERY_SET:-all}"
  query_label="${query_label,,}"
  query_label="${query_label//[^a-z0-9,]/-}"
  if [[ -n "${RESULTS_RUN_ID}" ]]; then
    COMPARE_PARENT_RUN_ID="${RESULTS_RUN_ID}"
  else
    COMPARE_PARENT_RUN_ID="${BENCHMARK}-compare-${query_label}-$(date +%Y%m%d-%H%M%S)"
  fi
  RESULTS_RUN_ID="${COMPARE_PARENT_RUN_ID}"
  prepare_results_dir
  prepare_metadata_output
  init_machine_result "${RESULTS_DIR}"
}

normalize_benchmark

if [[ "${BENCHMARK}" == "help" || "${BENCHMARK}" == "--help" || "${BENCHMARK}" == "-h" || "${MODE}" == "help" || "${MODE}" == "--help" || "${MODE}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${BENCHMARK}" != "tpch" && "${BENCHMARK}" != "tpcds" ]]; then
  usage
  exit 2
fi

# Auto-select BenchBase image for TPC-DS unless explicitly overridden.
if [[ "${BENCHMARK}" == "tpcds" && -z "${BENCHBASE_IMAGE:-}" ]]; then
  BENCHBASE_IMAGE="benchbase-tpcds:latest"
fi
export BENCHBASE_IMAGE

if [[ "${BENCHMARK}" == "tpcds" ]] && ! docker image inspect "${BENCHBASE_IMAGE}" >/dev/null 2>&1; then
  echo "ERROR: TPC-DS BenchBase image '${BENCHBASE_IMAGE}' not found." >&2
  echo "Build it with:" >&2
  echo "  bash benchmarks/benchbase-spark/tpcds/build.sh" >&2
  exit 2
fi

if [[ "${MODE}" == "smoke" && "${BENCHMARK}" == "tpch" && -z "${QUERY_SET}" ]]; then
  QUERY_SET="q6"
fi

if [[ "${MODE}" == "smoke" && "${BENCHMARK}" == "tpcds" && -z "${QUERY_SET}" ]]; then
  QUERY_SET="q4"
fi

if [[ "${MODE}" == "graph" || "${MODE}" == "compare" ]]; then
  if [[ "${BENCHMARK}" == "tpch" ]]; then
    QUERY_SET="${QUERY_SET:-q6}"
  elif [[ "${BENCHMARK}" == "tpcds" ]]; then
    QUERY_SET="${QUERY_SET:-q4}"
  fi
  if [[ "${QUERY_SET,,}" == "all" && -z "${BENCHBASE_TIME_SECONDS}" ]]; then
    BENCHBASE_WARMUP_SECONDS="${BENCHBASE_WARMUP_SECONDS:-0}"
  else
    BENCHBASE_TIME_SECONDS="${BENCHBASE_TIME_SECONDS:-60}"
    BENCHBASE_WARMUP_SECONDS="${BENCHBASE_WARMUP_SECONDS:-30}"
  fi
  BENCHBASE_TERMINALS="${BENCHBASE_TERMINALS:-1}"
fi

if [[ "${MODE}" == "compare" ]]; then
  BENCHBASE_COMPARE_ORDER="${BENCHBASE_COMPARE_ORDER,,}"
  BENCHBASE_CACHE_POLICY="${BENCHBASE_CACHE_POLICY,,}"
  if [[ "${BENCHBASE_COMPARE_ORDER}" != "flight-first"
      && "${BENCHBASE_COMPARE_ORDER}" != "direct-first" ]]; then
    echo "BENCHBASE_COMPARE_ORDER must be flight-first or direct-first: ${BENCHBASE_COMPARE_ORDER}" >&2
    exit 2
  fi
  if [[ ! "${BENCHBASE_PAIRED_OBSERVATIONS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCHBASE_PAIRED_OBSERVATIONS must be a positive integer: ${BENCHBASE_PAIRED_OBSERVATIONS}" >&2
    exit 2
  fi
  if [[ "${BENCHBASE_CACHE_POLICY}" != "warm-cache" ]]; then
    echo "BENCHBASE_CACHE_POLICY currently supports warm-cache only: ${BENCHBASE_CACHE_POLICY}" >&2
    exit 2
  fi
fi

configure_cluster

case "${MODE}" in
  smoke|fresh|graph)
    prepare_results_dir
    prepare_metadata_output
    reset_stack
    prepare_stack
    benchbase_execute
    ;;
  prepare)
    prepare_results_dir
    prepare_metadata_output
    reset_stack
    prepare_stack
    ;;
  compare)
    init_compare_run
    reset_stack
    prepare_compare_stack
    compare_execute
    ;;
  prepare-compare)
    init_compare_run
    reset_stack
    prepare_compare_stack
    ;;
  run)
    BENCHBASE_DB_SCHEMA="${BENCHMARK}"
    start_observability
    start_thrift
    verify_spark_flight
    benchbase_execute
    ;;
  run-flight)
    BENCHBASE_DB_SCHEMA="${BENCHMARK}_flight"
    start_observability
    start_thrift
    verify_spark_schema "${BENCHBASE_DB_SCHEMA}" "FlightSource"
    benchbase_execute
    ;;
  run-direct)
    BENCHBASE_DB_SCHEMA="${BENCHMARK}_direct"
    start_observability
    start_thrift
    verify_spark_schema "${BENCHBASE_DB_SCHEMA}" "direct Parquet"
    benchbase_execute
    ;;
  report)
    RESULTS_DIR="${RESULTS_ROOT}"
    build_html_report
    ;;
  pages)
    build_pages_site
    ;;
  down)
    reset_stack
    ;;
  logs)
    compose --profile benchbase logs -f
    ;;
  help|--help|-h)
    usage
    ;;
  *)
    usage
    exit 2
    ;;
esac
