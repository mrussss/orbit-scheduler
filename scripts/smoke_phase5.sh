#!/usr/bin/env bash
set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work_dir=$(mktemp -d /tmp/orbit-phase5-smoke-XXXXXX)
container_name="orbit-phase5-smoke-$$"
server_pid=""
worker_pids=()

cleanup() {
  status=$?
  trap - EXIT
  set +e
  for pid in "${worker_pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null
  done
  if [[ -n "$server_pid" ]]; then
    kill -TERM "$server_pid" 2>/dev/null
  fi
  for pid in "${worker_pids[@]}"; do
    wait "$pid" 2>/dev/null
  done
  if [[ -n "$server_pid" ]]; then
    wait "$server_pid" 2>/dev/null
  fi
  docker rm -f "$container_name" >/dev/null 2>&1
  if (( status != 0 )); then
    echo "phase5 smoke failed; recent server log:" >&2
    tail -n 40 "$work_dir/server.log" 2>/dev/null >&2
    for log_file in "$work_dir"/worker-*.log; do
      [[ -f "$log_file" ]] && tail -n 20 "$log_file" >&2
    done
  fi
  rm -rf "$work_dir"
  exit "$status"
}
trap cleanup EXIT

for command_name in curl docker go sed; do
  command -v "$command_name" >/dev/null || {
    echo "required command not found: $command_name" >&2
    exit 1
  }
done

http_port=${SMOKE_HTTP_PORT:-18080}
grpc_port=${SMOKE_GRPC_PORT:-19090}
metrics_port=${SMOKE_METRICS_PORT:-19091}
admin_token=smoke-admin-token-with-at-least-32-characters
token_pepper=smoke-token-pepper-with-at-least-32-characters

cd "$root_dir"
go build -o "$work_dir/orbit-server" ./cmd/orbit-server
go build -o "$work_dir/orbit-worker" ./cmd/orbit-worker
go build -o "$work_dir/orbit-migrate" ./cmd/orbit-migrate

docker run --rm -d \
  --name "$container_name" \
  -e POSTGRES_USER=orbit \
  -e POSTGRES_PASSWORD=orbit \
  -e POSTGRES_DB=orbit \
  -p 127.0.0.1::5432 \
  --health-cmd='pg_isready -U orbit -d orbit' \
  --health-interval=1s \
  --health-timeout=3s \
  --health-retries=30 \
  postgres:16-alpine >/dev/null

for _ in $(seq 1 60); do
  [[ $(docker inspect -f '{{.State.Health.Status}}' "$container_name") == healthy ]] && break
  sleep 0.5
done
[[ $(docker inspect -f '{{.State.Health.Status}}' "$container_name") == healthy ]]
postgres_port=$(docker port "$container_name" 5432/tcp | sed -n 's/.*://p')
database_url="postgres://orbit:orbit@127.0.0.1:${postgres_port}/orbit?sslmode=disable"
DATABASE_URL="$database_url" "$work_dir/orbit-migrate" up

DATABASE_URL="$database_url" \
TOKEN_PEPPER="$token_pepper" \
ADMIN_TOKEN="$admin_token" \
HTTP_ADDR="127.0.0.1:${http_port}" \
GRPC_ADDR="127.0.0.1:${grpc_port}" \
METRICS_ADDR="127.0.0.1:${metrics_port}" \
"$work_dir/orbit-server" >"$work_dir/server.log" 2>&1 &
server_pid=$!

for _ in $(seq 1 100); do
  curl --silent --fail "http://127.0.0.1:${http_port}/health/ready" >/dev/null && break
  kill -0 "$server_pid" 2>/dev/null || exit 1
  sleep 0.1
done
curl --silent --fail "http://127.0.0.1:${http_port}/health/ready" >/dev/null

project_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${admin_token}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"phase5-smoke","task_quota":100,"max_concurrent_tasks":4}' \
  "http://127.0.0.1:${http_port}/api/v1/projects")
project_id=$(printf '%s' "$project_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$project_id" ]]

token_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${admin_token}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"phase5-smoke","scopes":["task:read","task:write"]}' \
  "http://127.0.0.1:${http_port}/api/v1/projects/${project_id}/tokens")
project_token=$(printf '%s' "$token_json" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[[ -n "$project_token" ]]

for worker_number in 1 2; do
  GRPC_ADDR="127.0.0.1:${grpc_port}" \
  WORKER_NAME="phase5-smoke-${worker_number}" \
  WORKER_TASK_TYPES=mock \
  WORKER_CAPACITY=2 \
  WORKER_FETCH_INTERVAL=50ms \
  WORKER_HEARTBEAT_INTERVAL=100ms \
  WORKER_LEASE_DURATION=500ms \
  WORKER_RENEW_INTERVAL=100ms \
  WORKER_GRACE_PERIOD=3s \
  "$work_dir/orbit-worker" >"$work_dir/worker-${worker_number}.log" 2>&1 &
  worker_pids+=("$!")
done

task_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: phase5-smoke-task' \
  -d '{"task_type":"mock","payload":{"mode":"delay_success","delay_ms":700,"result":{"smoke":true}},"priority":10,"execution_timeout_ms":3000,"max_attempts":2}' \
  "http://127.0.0.1:${http_port}/api/v1/tasks")
task_id=$(printf '%s' "$task_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$task_id" ]]

task_status=""
for _ in $(seq 1 150); do
  task_json=$(curl --silent --show-error --fail-with-body \
    -H "Authorization: Bearer ${project_token}" \
    "http://127.0.0.1:${http_port}/api/v1/tasks/${task_id}")
  task_status=$(printf '%s' "$task_json" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
  [[ "$task_status" == SUCCEEDED ]] && break
  [[ "$task_status" == FAILED || "$task_status" == CANCELED ]] && break
  sleep 0.1
done
[[ "$task_status" == SUCCEEDED ]]

result_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  "http://127.0.0.1:${http_port}/api/v1/tasks/${task_id}/result")
[[ "$result_json" == *'"smoke":true'* ]]
attempts_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  "http://127.0.0.1:${http_port}/api/v1/tasks/${task_id}/attempts")
[[ "$attempts_json" == *'"attempt_no":1'* ]]

for pid in "${worker_pids[@]}"; do
  kill -TERM "$pid"
done
for pid in "${worker_pids[@]}"; do
  for _ in $(seq 1 100); do
    state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    sleep 0.1
  done
  state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
  if [[ -n "$state" && "$state" != Z* ]]; then
    echo "worker $pid did not stop within 10 seconds" >&2
    exit 1
  fi
  wait "$pid"
done
worker_pids=()

echo "phase5 smoke passed project_id=${project_id} task_id=${task_id} status=${task_status} attempts=1 workers=2"
