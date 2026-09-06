#!/usr/bin/env bash
# Run the immutable upstream Northwind Bolt workload against NornicDB and CopperDB.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NORNICDB_ROOT="${NORNICDB_ROOT:-${HOME}/src/NornicDB}"
ITERATIONS="${ITERATIONS:-10}"
WARMUP="${WARMUP:-2}"
CATEGORIES="${CATEGORIES:-96}"
SUPPLIERS="${SUPPLIERS:-144}"
CUSTOMERS="${CUSTOMERS:-1200}"
PRODUCTS="${PRODUCTS:-48000}"
ORDERS="${ORDERS:-48000}"
ORDER_LINES_MIN="${ORDER_LINES_MIN:-1}"
ORDER_LINES_MAX="${ORDER_LINES_MAX:-6}"
BATCH_SIZE="${BATCH_SIZE:-500}"
PARALLEL="${PARALLEL:-4}"
SEED="${SEED:-42}"
GRAPH_ONLY="${GRAPH_ONLY:-1}"
POWER_METRICS="${POWER_METRICS:-auto}"
REPORT_ROOT="${REPORT_ROOT:-${REPO_ROOT}/docs/performance/1.1.0-northwind-results}"
RUN_ID="${RUN_ID:-run-$(date +%Y%m%d_%H%M%S)}"
REPORT_DIR="${REPORT_DIR:-${REPORT_ROOT}/${RUN_ID}}"
WORK_DIR="${WORK_DIR:-${REPO_ROOT}/target/northwind-benchmark}"
NORNIC_DATA_DIR="${NORNIC_DATA_DIR:-${WORK_DIR}/nornicdb}"
COPPER_STORAGE_ROOT="${COPPER_STORAGE_ROOT:-${WORK_DIR}/copperdb-data}"
COPPER_AUTH_STORAGE_ROOT="${COPPER_AUTH_STORAGE_ROOT:-${WORK_DIR}/copperdb-auth}"
COPPER_DATABASE="${COPPER_DATABASE:-copperdb}"
NORNIC_DATABASE="${NORNIC_DATABASE:-nornic}"
NORNIC_BOLT_PORT="${NORNIC_BOLT_PORT:-17687}"
NORNIC_HTTP_PORT="${NORNIC_HTTP_PORT:-17474}"
COPPER_BOLT_PORT="${COPPER_BOLT_PORT:-17688}"
COPPER_HTTP_PORT="${COPPER_HTTP_PORT:-17475}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "error: $*"; exit 1; }

NORNIC_PID=""
COPPER_PID=""
VMSTAT_PID=""
POWER_PID=""
SUDO_KEEPALIVE_PID=""
cleanup() {
  local exit_code=$?
  set +e
  [[ -n "${NORNIC_PID}" ]] && kill "${NORNIC_PID}" 2>/dev/null || true
  [[ -n "${COPPER_PID}" ]] && kill "${COPPER_PID}" 2>/dev/null || true
  [[ -n "${POWER_PID}" ]] && sudo kill -KILL "${POWER_PID}" 2>/dev/null || true
  [[ -n "${VMSTAT_PID}" ]] && kill -KILL "${VMSTAT_PID}" 2>/dev/null || true
  [[ -n "${SUDO_KEEPALIVE_PID}" ]] && kill "${SUDO_KEEPALIVE_PID}" 2>/dev/null || true
  wait "${NORNIC_PID}" 2>/dev/null || true
  wait "${COPPER_PID}" 2>/dev/null || true
  rm -rf "${NORNIC_DATA_DIR}" "${COPPER_STORAGE_ROOT}" "${COPPER_AUTH_STORAGE_ROOT}"
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

require() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
require go
require python3
require nc
[[ -d "${NORNICDB_ROOT}" ]] || die "NORNICDB_ROOT does not exist: ${NORNICDB_ROOT}"
if [[ "${POWER_METRICS}" == "1" ]]; then
  require sudo
  [[ -x /usr/bin/powermetrics ]] || die "/usr/bin/powermetrics is required when POWER_METRICS=1"
  sudo -v
  (while true; do sudo -n true 2>/dev/null; sleep 50; done) &
  SUDO_KEEPALIVE_PID=$!
elif [[ "${POWER_METRICS}" == "auto" ]]; then
  if command -v sudo >/dev/null 2>&1 && [[ -x /usr/bin/powermetrics ]] && sudo -n true 2>/dev/null; then
    POWER_METRICS=1
    (while true; do sudo -n true 2>/dev/null; sleep 50; done) &
    SUDO_KEEPALIVE_PID=$!
  else
    POWER_METRICS=0
    log "powermetrics unavailable without interactive sudo; continuing without power samples (set POWER_METRICS=1 after sudo -v to require them)"
  fi
elif [[ "${POWER_METRICS}" != "0" ]]; then
  die "POWER_METRICS must be auto, 0, or 1"
fi

mkdir -p "${WORK_DIR}" "${REPORT_DIR}"
rm -rf "${NORNIC_DATA_DIR}" "${COPPER_STORAGE_ROOT}" "${COPPER_AUTH_STORAGE_ROOT}"
rm -f "${REPORT_DIR}"/{nornicdb,copperdb}.{results.json,powermetrics.plist,vmstat.log,disk_bytes.txt,disk_human.txt,data_dir.txt,wall_seconds.txt,stdout.log,stderr.log,bench.log,md} "${REPORT_DIR}/comparison.md"

log "building immutable upstream Northwind workload"
(cd "${NORNICDB_ROOT}" && go build -o "${WORK_DIR}/northwind_power_bench" ./testing/benchmarks/northwind_power)
log "building NornicDB"
(cd "${NORNICDB_ROOT}" && go build -o "${WORK_DIR}/nornicdb-server" ./cmd/nornicdb)
log "building CopperDB in release mode"
(cd "${REPO_ROOT}" && cargo build -p copperdb --release)

cat > "${WORK_DIR}/copperdb.toml" <<EOF
[storage]
path = "${COPPER_STORAGE_ROOT}"
EOF

wait_for_bolt() {
  local pid="$1"
  local port="$2"
  local name="$3"
  for _ in $(seq 1 60); do
    nc -z 127.0.0.1 "${port}" 2>/dev/null && return 0
    kill -0 "${pid}" 2>/dev/null || die "${name} exited during startup; inspect ${WORK_DIR}/${name}.stderr.log"
    sleep 1
  done
  die "${name} Bolt port ${port} never became ready"
}

start_measurement() {
  local label="$1"
  VMSTAT_PID=""
  POWER_PID=""
  vm_stat 1 > "${REPORT_DIR}/${label}.vmstat.log" 2>&1 &
  VMSTAT_PID=$!
  if [[ "${POWER_METRICS}" == "1" ]]; then
    sudo /usr/bin/powermetrics --samplers cpu_power,gpu_power -i 1000 -f plist \
      -o "${REPORT_DIR}/${label}.powermetrics.plist" >/dev/null 2>&1 &
    POWER_PID=$!
  fi
}

stop_measurement() {
  if [[ -n "${POWER_PID}" ]]; then
    sudo kill -INT "${POWER_PID}" 2>/dev/null || true
    for _ in {1..12}; do
      kill -0 "${POWER_PID}" 2>/dev/null || break
      sleep 0.25
    done
    kill -0 "${POWER_PID}" 2>/dev/null && sudo kill -KILL "${POWER_PID}" 2>/dev/null || true
    wait "${POWER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${VMSTAT_PID}" ]]; then
    # Background jobs inherit ignored SIGINT from Bash, so terminate explicitly.
    kill -TERM "${VMSTAT_PID}" 2>/dev/null || true
    for _ in {1..6}; do
      kill -0 "${VMSTAT_PID}" 2>/dev/null || break
      sleep 0.25
    done
    kill -0 "${VMSTAT_PID}" 2>/dev/null && kill -KILL "${VMSTAT_PID}" 2>/dev/null || true
    wait "${VMSTAT_PID}" 2>/dev/null || true
  fi
  POWER_PID=""
  VMSTAT_PID=""
}

record_file_inventory() {
  local data_dir="$1"
  local output_file="$2"
  find "${data_dir}" -type f -exec stat -f '%z\t%b\t%N' {} + | sort -nr > "${output_file}"
}

run_workload() {
  local label="$1"
  local uri="$2"
  local database="$3"
  local result_file="$4"
  local log_file="$5"
  log "running immutable Northwind workload for ${label}; seed and query progress follows"
  "${WORK_DIR}/northwind_power_bench" \
    -uri "${uri}" \
    -no-auth \
    -database "${database}" \
    -categories "${CATEGORIES}" \
    -suppliers "${SUPPLIERS}" \
    -customers "${CUSTOMERS}" \
    -products "${PRODUCTS}" \
    -orders "${ORDERS}" \
    -order-lines-min "${ORDER_LINES_MIN}" \
    -order-lines-max "${ORDER_LINES_MAX}" \
    -batch-size "${BATCH_SIZE}" \
    -parallel "${PARALLEL}" \
    -seed "${SEED}" \
    -iterations "${ITERATIONS}" \
    -warmup "${WARMUP}" \
    -label "${label}" \
    -out "${result_file}" \
    2> >(tee "${log_file}" >&2)
}

run_nornic() {
  log "starting NornicDB"
  local graph_only_flags=()
  if [[ "${GRAPH_ONLY}" == "1" ]]; then
    graph_only_flags=(--search-bm25-enabled=false --search-vector-enabled=false)
  fi
  start_measurement nornicdb
  local started_at="$(date +%s.%N)"
  NORNICDB_NO_AUTH=true NORNICDB_EMBEDDING_ENABLED=false \
    "${WORK_DIR}/nornicdb-server" serve --bolt-port "${NORNIC_BOLT_PORT}" --http-port "${NORNIC_HTTP_PORT}" \
    --data-dir "${NORNIC_DATA_DIR}" --no-auth "${graph_only_flags[@]}" \
    >"${REPORT_DIR}/nornicdb.stdout.log" 2>"${REPORT_DIR}/nornicdb.stderr.log" &
  NORNIC_PID=$!
  wait_for_bolt "${NORNIC_PID}" "${NORNIC_BOLT_PORT}" nornicdb
  run_workload nornicdb "bolt://127.0.0.1:${NORNIC_BOLT_PORT}" "${NORNIC_DATABASE}" "${REPORT_DIR}/nornicdb.results.json" \
    "${REPORT_DIR}/nornicdb.bench.log"
  kill -TERM "${NORNIC_PID}"
  wait "${NORNIC_PID}" || true
  NORNIC_PID=""
  stop_measurement
  local finished_at="$(date +%s.%N)"
  python3 -c "print(f'{float(${finished_at}) - float(${started_at}):.3f}')" > "${REPORT_DIR}/nornicdb.wall_seconds.txt"
  sync
  du -sk "${NORNIC_DATA_DIR}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/nornicdb.disk_bytes.txt"
  du -sh "${NORNIC_DATA_DIR}" > "${REPORT_DIR}/nornicdb.disk_human.txt"
  printf '%s\n' "${NORNIC_DATA_DIR}" > "${REPORT_DIR}/nornicdb.data_dir.txt"
  record_file_inventory "${NORNIC_DATA_DIR}" "${REPORT_DIR}/nornicdb.files.tsv"
}

run_copperdb() {
  log "starting CopperDB release binary"
  start_measurement copperdb
  local started_at="$(date +%s.%N)"
  COPPERDB_AUTH_STORAGE_PATH="${COPPER_AUTH_STORAGE_ROOT}" \
    "${REPO_ROOT}/target/release/copperdb" --config "${WORK_DIR}/copperdb.toml" --no-auth --headless \
    --bolt-port "${COPPER_BOLT_PORT}" --http-port "${COPPER_HTTP_PORT}" --db-name "${COPPER_DATABASE}" \
    >"${REPORT_DIR}/copperdb.stdout.log" 2>"${REPORT_DIR}/copperdb.stderr.log" &
  COPPER_PID=$!
  wait_for_bolt "${COPPER_PID}" "${COPPER_BOLT_PORT}" copperdb
  run_workload copperdb "bolt://127.0.0.1:${COPPER_BOLT_PORT}" "${COPPER_DATABASE}" "${REPORT_DIR}/copperdb.results.json" \
    "${REPORT_DIR}/copperdb.bench.log"
  kill -TERM "${COPPER_PID}"
  wait "${COPPER_PID}" || true
  COPPER_PID=""
  stop_measurement
  local finished_at="$(date +%s.%N)"
  python3 -c "print(f'{float(${finished_at}) - float(${started_at}):.3f}')" > "${REPORT_DIR}/copperdb.wall_seconds.txt"
  local copper_data_dir="${COPPER_STORAGE_ROOT}/${COPPER_DATABASE}"
  sync
  du -sk "${copper_data_dir}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/copperdb.disk_bytes.txt"
  du -sh "${copper_data_dir}" > "${REPORT_DIR}/copperdb.disk_human.txt"
  printf '%s\n' "${copper_data_dir}" > "${REPORT_DIR}/copperdb.data_dir.txt"
  record_file_inventory "${copper_data_dir}" "${REPORT_DIR}/copperdb.files.tsv"
}

run_nornic
run_copperdb
python3 "${SCRIPT_DIR}/northwind_report_nornic_copper.py" --dir "${REPORT_DIR}" --iterations "${ITERATIONS}" --products "${PRODUCTS}" --orders "${ORDERS}" --upstream-root "${NORNICDB_ROOT}"
log "results written to ${REPORT_DIR}"