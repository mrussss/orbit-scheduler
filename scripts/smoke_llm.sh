#!/usr/bin/env bash
set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work_dir=$(mktemp -d /tmp/orbit-llm-smoke-XXXXXX)
container_name="orbit-llm-smoke-$$"
server_pid=""
worker_pid=""
provider_pid=""

cleanup() {
  status=$?
  trap - EXIT
  set +e
  [[ -n "$worker_pid" ]] && kill -TERM "$worker_pid" 2>/dev/null
  [[ -n "$server_pid" ]] && kill -TERM "$server_pid" 2>/dev/null
  [[ -n "$provider_pid" ]] && kill -TERM "$provider_pid" 2>/dev/null
  [[ -n "$worker_pid" ]] && wait "$worker_pid" 2>/dev/null
  [[ -n "$server_pid" ]] && wait "$server_pid" 2>/dev/null
  [[ -n "$provider_pid" ]] && wait "$provider_pid" 2>/dev/null
  docker rm -f "$container_name" >/dev/null 2>&1
  if (( status != 0 )); then
    echo "LLM smoke failed; recent logs:" >&2
    tail -n 40 "$work_dir/server.log" 2>/dev/null >&2
    tail -n 40 "$work_dir/worker.log" 2>/dev/null >&2
    tail -n 40 "$work_dir/provider.log" 2>/dev/null >&2
  fi
  rm -rf "$work_dir"
  exit "$status"
}
trap cleanup EXIT

for command_name in curl docker go grep sed; do
  command -v "$command_name" >/dev/null || {
    echo "required command not found: $command_name" >&2
    exit 1
  }
done

http_port=${SMOKE_LLM_HTTP_PORT:-28080}
grpc_port=${SMOKE_LLM_GRPC_PORT:-29090}
server_metrics_port=${SMOKE_LLM_SERVER_METRICS_PORT:-29091}
worker_metrics_port=${SMOKE_LLM_WORKER_METRICS_PORT:-29092}
provider_port=${SMOKE_LLM_PROVIDER_PORT:-28089}
admin_token=llm-smoke-admin-token-with-at-least-32-characters
token_pepper=llm-smoke-token-pepper-with-at-least-32-characters
provider_key=fake-provider-key

cd "$root_dir"
go build -o "$work_dir/orbit-server" ./cmd/orbit-server
go build -o "$work_dir/orbit-worker" ./cmd/orbit-worker
go build -o "$work_dir/orbit-migrate" ./cmd/orbit-migrate
go build -o "$work_dir/fake-llm-provider" ./scripts

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

for _ in $(seq 1 20); do
  if DATABASE_URL="$database_url" "$work_dir/orbit-migrate" up >"$work_dir/migrate.log" 2>&1; then
    break
  fi
  sleep 0.5
done
DATABASE_URL="$database_url" "$work_dir/orbit-migrate" up >/dev/null

FAKE_LLM_ADDR="127.0.0.1:${provider_port}" \
FAKE_LLM_API_KEY="$provider_key" \
"$work_dir/fake-llm-provider" >"$work_dir/provider.log" 2>&1 &
provider_pid=$!
for _ in $(seq 1 100); do
  curl --silent --fail "http://127.0.0.1:${provider_port}/health" >/dev/null && break
  kill -0 "$provider_pid" 2>/dev/null || exit 1
  sleep 0.1
done
curl --silent --fail "http://127.0.0.1:${provider_port}/health" >/dev/null

DATABASE_URL="$database_url" \
TOKEN_PEPPER="$token_pepper" \
ADMIN_TOKEN="$admin_token" \
HTTP_ADDR="127.0.0.1:${http_port}" \
GRPC_ADDR="127.0.0.1:${grpc_port}" \
METRICS_ADDR="127.0.0.1:${server_metrics_port}" \
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
  -d '{"name":"llm-smoke","task_quota":100,"max_concurrent_tasks":2}' \
  "http://127.0.0.1:${http_port}/api/v1/projects")
project_id=$(printf '%s' "$project_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$project_id" ]]

token_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${admin_token}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"llm-smoke","scopes":["task:read","task:write"]}' \
  "http://127.0.0.1:${http_port}/api/v1/projects/${project_id}/tokens")
project_token=$(printf '%s' "$token_json" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[[ -n "$project_token" ]]

start_worker() {
  APP_ENV=test \
  GRPC_ADDR="127.0.0.1:${grpc_port}" \
  WORKER_NAME=llm-smoke-worker \
  WORKER_TASK_TYPES=llm \
  WORKER_CAPACITY=1 \
  WORKER_METRICS_ADDR="127.0.0.1:${worker_metrics_port}" \
  WORKER_FETCH_INTERVAL=50ms \
  WORKER_HEARTBEAT_INTERVAL=100ms \
  WORKER_LEASE_DURATION=2s \
  WORKER_RENEW_INTERVAL=500ms \
  WORKER_GRACE_PERIOD=500ms \
  LLM_BASE_URL="http://127.0.0.1:${provider_port}/v1" \
  LLM_API_KEY="$provider_key" \
  LLM_ALLOWED_MODELS=approved-model \
  LLM_REQUEST_TIMEOUT=20s \
  LLM_MAX_CONCURRENCY=1 \
  LLM_COST_TABLE_JSON='{"approved-model":{"prompt_microunits_per_million_tokens":1000000,"completion_microunits_per_million_tokens":1000000}}' \
  "$work_dir/orbit-worker" >>"$work_dir/worker.log" 2>&1 &
  worker_pid=$!

  for _ in $(seq 1 100); do
    curl --silent --fail "http://127.0.0.1:${worker_metrics_port}/metrics" >/dev/null && break
    kill -0 "$worker_pid" 2>/dev/null || exit 1
    sleep 0.1
  done
  curl --silent --fail "http://127.0.0.1:${worker_metrics_port}/metrics" >/dev/null
}

stop_worker() {
  kill -TERM "$worker_pid"
  for _ in $(seq 1 100); do
    state=$(ps -o stat= -p "$worker_pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    sleep 0.1
  done
  state=$(ps -o stat= -p "$worker_pid" 2>/dev/null || true)
  if [[ -n "$state" && "$state" != Z* ]]; then
    echo "LLM worker $worker_pid did not stop within 10 seconds" >&2
    exit 1
  fi
  wait "$worker_pid"
  worker_pid=""
}

start_worker

task_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: llm-smoke-retry' \
  -d '{"task_type":"llm","payload":{"model":"approved-model","messages":[{"role":"user","content":"retry-once smoke"}],"temperature":0.2,"max_output_tokens":100},"priority":10,"execution_timeout_ms":2000,"max_attempts":3}' \
  "http://127.0.0.1:${http_port}/api/v1/tasks")
task_id=$(printf '%s' "$task_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$task_id" ]]

task_status=""
for _ in $(seq 1 200); do
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
[[ "$result_json" == *'"provider":"openai-compatible"'* ]]
[[ "$result_json" == *'"total_tokens":12'* ]]
[[ "$result_json" == *'"estimated_cost_microunits":12'* ]]

attempts_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  "http://127.0.0.1:${http_port}/api/v1/tasks/${task_id}/attempts")
[[ "$attempts_json" == *'"attempt_no":1'* ]]
[[ "$attempts_json" == *'"attempt_no":2'* ]]

curl --silent --fail "http://127.0.0.1:${worker_metrics_port}/metrics" | grep -q 'orbit_llm_requests_total'
curl --silent --fail "http://127.0.0.1:${worker_metrics_port}/metrics" | grep -q 'orbit_llm_rate_limited_total'
curl --silent --fail "http://127.0.0.1:${server_metrics_port}/metrics" | grep -q 'orbit_scheduler_report_total'

shutdown_task_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: llm-smoke-shutdown' \
  -d '{"task_type":"llm","payload":{"model":"approved-model","messages":[{"role":"user","content":"slow-once shutdown smoke"}],"max_output_tokens":100},"priority":20,"execution_timeout_ms":15000,"max_attempts":3}' \
  "http://127.0.0.1:${http_port}/api/v1/tasks")
shutdown_task_id=$(printf '%s' "$shutdown_task_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$shutdown_task_id" ]]

shutdown_status=""
for _ in $(seq 1 100); do
  shutdown_task_json=$(curl --silent --show-error --fail-with-body \
    -H "Authorization: Bearer ${project_token}" \
    "http://127.0.0.1:${http_port}/api/v1/tasks/${shutdown_task_id}")
  shutdown_status=$(printf '%s' "$shutdown_task_json" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
  [[ "$shutdown_status" == RUNNING ]] && break
  sleep 0.1
done
[[ "$shutdown_status" == RUNNING ]]

stop_worker

shutdown_attempts_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  "http://127.0.0.1:${http_port}/api/v1/tasks/${shutdown_task_id}/attempts")
[[ "$shutdown_attempts_json" == *'"attempt_no":1'* ]]
[[ "$shutdown_attempts_json" == *'"outcome":"RETRYABLE_FAILURE"'* ]]

start_worker
shutdown_status=""
for _ in $(seq 1 200); do
  shutdown_task_json=$(curl --silent --show-error --fail-with-body \
    -H "Authorization: Bearer ${project_token}" \
    "http://127.0.0.1:${http_port}/api/v1/tasks/${shutdown_task_id}")
  shutdown_status=$(printf '%s' "$shutdown_task_json" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
  [[ "$shutdown_status" == SUCCEEDED ]] && break
  [[ "$shutdown_status" == FAILED || "$shutdown_status" == CANCELED ]] && break
  sleep 0.1
done
[[ "$shutdown_status" == SUCCEEDED ]]
shutdown_attempts_json=$(curl --silent --show-error --fail-with-body \
  -H "Authorization: Bearer ${project_token}" \
  "http://127.0.0.1:${http_port}/api/v1/tasks/${shutdown_task_id}/attempts")
[[ "$shutdown_attempts_json" == *'"attempt_no":1'* ]]
[[ "$shutdown_attempts_json" == *'"attempt_no":2'* ]]

if grep -R -q "$provider_key" "$work_dir"/*.log; then
  echo "provider API key leaked into logs" >&2
  exit 1
fi

stop_worker

echo "LLM smoke passed retry_task_id=${task_id} attempts=2 shutdown_task_id=${shutdown_task_id} shutdown_attempts=2 tokens=12 cost_microunits=12"
