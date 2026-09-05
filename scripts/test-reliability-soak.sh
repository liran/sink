#!/usr/bin/env bash
set -euo pipefail

server_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
suite_dir="${SINK_SUITE_DIR:?set SINK_SUITE_DIR to the pinned production-suite checkout}"
export SINK_SERVER_DIR="${server_dir}"
export SINK_SOAK_DURATION="${SINK_SOAK_DURATION:-2h}"
export SINK_SOAK_CONCURRENCY="${SINK_SOAK_CONCURRENCY:-16}"
export SINK_SOAK_MIN_CYCLES="${SINK_SOAK_MIN_CYCLES:-1000}"
export SINK_RUN_SOAK=1
export SINK_ADDRESS=127.0.0.1:18080
export SINK_SECONDARY_ADDRESS=127.0.0.1:18081
export SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200
export SINK_BACKEND_STORES='primary:async,secondary:async,sync-only:sync,elasticsearch-sync:sync,elasticsearch-async:async,mongodb-sync:sync,mongodb-async:async'

artifacts="$(mktemp -d "${TMPDIR:-/tmp}/sink-soak.XXXXXXXX")"
project="sink-soak-$(date +%s)-$$"
compose=(docker compose --project-name "${project}" --project-directory "${suite_dir}" --file "${suite_dir}/deploy/compose.yaml" --file "${artifacts}/override.yaml")
test_pid=""
sampler_pid=""
fault_pid=""

cleanup() {
  result="$?"
  trap - EXIT
  for process in "${sampler_pid}" "${fault_pid}" "${test_pid}"; do
    if [[ -n "${process}" ]]; then
      kill "${process}" 2>/dev/null || true
      wait "${process}" 2>/dev/null || true
    fi
  done
  "${compose[@]}" logs --no-color > "${artifacts}/containers.log" 2>&1 || true
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  echo "Reliability artifacts: ${artifacts}"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "artifacts=${artifacts}" >> "${GITHUB_OUTPUT}"
  fi
  exit "${result}"
}
trap cleanup EXIT

# Generate a test-only configuration; preserve the suite checkout. Removing
# the suite override exercises the candidate's product retry default.
sed '/^[[:space:]]*max_retry_attempts:/d' "${suite_dir}/deploy/worker.yaml" > "${artifacts}/worker.yaml"
cat >> "${artifacts}/worker.yaml" <<'YAML'
prometheus:
  address: ":9090"
YAML
cat > "${artifacts}/override.yaml" <<YAML
services:
  sink-worker:
    volumes:
      - ${artifacts}/worker.yaml:/etc/sink/config.yaml:ro
    ports:
      - 127.0.0.1:19092:9090
YAML
git -C "${server_dir}" rev-parse HEAD > "${artifacts}/server-revision.txt"
git -C "${suite_dir}" rev-parse HEAD > "${artifacts}/suite-revision.txt"
"${compose[@]}" config > "${artifacts}/compose.yaml"
"${compose[@]}" up --build --detach --wait --wait-timeout 180
for port in 19090 19091 19092; do
  healthy=0
  for attempt in $(seq 1 60); do
    if curl --max-time 4 --fail --silent "http://127.0.0.1:${port}/readyz" >/dev/null; then
      healthy=1
      break
    fi
    sleep 2
  done
  if [[ "${healthy}" != 1 ]]; then
    echo "Sink readiness on port ${port} did not recover" >&2
    exit 1
  fi
done

(
  while true; do
    "${compose[@]}" stats --no-stream --format json >> "${artifacts}/resources.jsonl" || true
    for port in 19090 19091 19092; do
      date -u +%FT%TZ >> "${artifacts}/metrics-${port}.txt"
      curl --max-time 3 --silent "http://127.0.0.1:${port}/metrics" >> "${artifacts}/metrics-${port}.txt" || true
    done
    sleep 30
  done
) &
sampler_pid="$!"

(
  # Fail active consumers and hold a backend unavailable beyond one processing
  # window; these are unique, disposable Compose resources for this test.
  sleep 30
  date -u +%FT%TZ >> "${artifacts}/faults.log"
  "${compose[@]}" kill --signal SIGKILL sink-worker >> "${artifacts}/faults.log" 2>&1
  "${compose[@]}" up --detach sink-worker >> "${artifacts}/faults.log" 2>&1
  sleep 30
  date -u +%FT%TZ >> "${artifacts}/faults.log"
  "${compose[@]}" stop opensearch >> "${artifacts}/faults.log" 2>&1
  sleep 45
  "${compose[@]}" start opensearch >> "${artifacts}/faults.log" 2>&1
  sleep 60
  date -u +%FT%TZ >> "${artifacts}/faults.log"
  "${compose[@]}" restart kafka >> "${artifacts}/faults.log" 2>&1
) &
fault_pid="$!"

(
  cd "${suite_dir}"
  go test -tags=integration ./integration -run '^TestStorageBackendSoak$' -count=1 -timeout=150m -v
) > "${artifacts}/test.log" 2>&1 &
test_pid="$!"
wait "${test_pid}"
test_pid=""
wait "${fault_pid}"
fault_pid=""
cat "${artifacts}/test.log"
