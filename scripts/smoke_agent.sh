#!/usr/bin/env bash
set -Eeuo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work_dir=$(mktemp -d /tmp/orbit-agent-smoke-XXXXXX)
container_name="orbit-agent-smoke-$$"
server_pid=""
worker_pid=""
provider_pid=""
sse_pid=""

cleanup() {
  status=$?
  trap - EXIT
  set +e
  [[ -n "$sse_pid" ]] && kill -TERM "$sse_pid" 2>/dev/null
  [[ -n "$worker_pid" ]] && kill -TERM "$worker_pid" 2>/dev/null
  [[ -n "$server_pid" ]] && kill -TERM "$server_pid" 2>/dev/null
  [[ -n "$provider_pid" ]] && kill -TERM "$provider_pid" 2>/dev/null
  [[ -n "$sse_pid" ]] && wait "$sse_pid" 2>/dev/null
  [[ -n "$worker_pid" ]] && wait "$worker_pid" 2>/dev/null
  [[ -n "$server_pid" ]] && wait "$server_pid" 2>/dev/null
  [[ -n "$provider_pid" ]] && wait "$provider_pid" 2>/dev/null
  docker rm -f "$container_name" >/dev/null 2>&1
  if (( status != 0 )); then
    echo "Agent smoke failed; recent logs:" >&2
    tail -n 60 "$work_dir/server.log" 2>/dev/null >&2
    tail -n 60 "$work_dir/worker.log" 2>/dev/null >&2
    tail -n 60 "$work_dir/provider.log" 2>/dev/null >&2
    for events in "$work_dir"/*.events; do
      [[ -f "$events" ]] && { echo "--- $events" >&2; tail -n 80 "$events" >&2; }
    done
  fi
  rm -rf "$work_dir"
  exit "$status"
}
trap cleanup EXIT

for command_name in curl docker go grep sed; do
  command -v "$command_name" >/dev/null || { echo "required command not found: $command_name" >&2; exit 1; }
done

http_port=${SMOKE_AGENT_HTTP_PORT:-38080}
grpc_port=${SMOKE_AGENT_GRPC_PORT:-39090}
server_metrics_port=${SMOKE_AGENT_SERVER_METRICS_PORT:-39091}
worker_metrics_port=${SMOKE_AGENT_WORKER_METRICS_PORT:-39092}
provider_port=${SMOKE_AGENT_PROVIDER_PORT:-38089}
admin_token=agent-smoke-admin-token-with-at-least-32-characters
token_pepper=agent-smoke-token-pepper-with-at-least-32-characters
provider_key=fake-agent-provider-key

cd "$root_dir"
go build -o "$work_dir/orbit-server" ./cmd/orbit-server
go build -o "$work_dir/orbit-worker" ./cmd/orbit-worker
go build -o "$work_dir/orbit-migrate" ./cmd/orbit-migrate
go build -o "$work_dir/fake-llm-provider" ./scripts

docker run --rm -d \
  --name "$container_name" \
  -e POSTGRES_USER=orbit -e POSTGRES_PASSWORD=orbit -e POSTGRES_DB=orbit \
  -p 127.0.0.1::5432 \
  --health-cmd='pg_isready -U orbit -d orbit' --health-interval=1s --health-timeout=3s --health-retries=30 \
  postgres:16-alpine >/dev/null

for _ in $(seq 1 60); do
  [[ $(docker inspect -f '{{.State.Health.Status}}' "$container_name") == healthy ]] && break
  sleep 0.5
done
[[ $(docker inspect -f '{{.State.Health.Status}}' "$container_name") == healthy ]]
postgres_port=$(docker port "$container_name" 5432/tcp | sed -n 's/.*://p')
database_url="postgres://orbit:orbit@127.0.0.1:${postgres_port}/orbit?sslmode=disable"

for _ in $(seq 1 20); do
  if DATABASE_URL="$database_url" "$work_dir/orbit-migrate" up >"$work_dir/migrate.log" 2>&1; then break; fi
  sleep 0.5
done
DATABASE_URL="$database_url" "$work_dir/orbit-migrate" up >/dev/null

FAKE_LLM_ADDR="127.0.0.1:${provider_port}" FAKE_LLM_API_KEY="$provider_key" \
  "$work_dir/fake-llm-provider" >"$work_dir/provider.log" 2>&1 &
provider_pid=$!
for _ in $(seq 1 100); do
  curl --silent --fail "http://127.0.0.1:${provider_port}/health" >/dev/null && break
  kill -0 "$provider_pid" 2>/dev/null || exit 1
  sleep 0.1
done
curl --silent --fail "http://127.0.0.1:${provider_port}/health" >/dev/null

DATABASE_URL="$database_url" TOKEN_PEPPER="$token_pepper" ADMIN_TOKEN="$admin_token" \
HTTP_ADDR="127.0.0.1:${http_port}" GRPC_ADDR="127.0.0.1:${grpc_port}" METRICS_ADDR="127.0.0.1:${server_metrics_port}" \
  "$work_dir/orbit-server" >"$work_dir/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 100); do
  curl --silent --fail "http://127.0.0.1:${http_port}/health/ready" >/dev/null && break
  kill -0 "$server_pid" 2>/dev/null || exit 1
  sleep 0.1
done
curl --silent --fail "http://127.0.0.1:${http_port}/health/ready" >/dev/null

project_json=$(curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${admin_token}" -H 'Content-Type: application/json' \
  -d '{"name":"agent-smoke","task_quota":100,"max_concurrent_tasks":2}' "http://127.0.0.1:${http_port}/api/v1/projects")
project_id=$(printf '%s' "$project_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$project_id" ]]
token_json=$(curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${admin_token}" -H 'Content-Type: application/json' \
  -d '{"name":"agent-smoke","scopes":["task:read","task:write"]}' "http://127.0.0.1:${http_port}/api/v1/projects/${project_id}/tokens")
project_token=$(printf '%s' "$token_json" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[[ -n "$project_token" ]]

start_worker() {
  APP_ENV=test GRPC_ADDR="127.0.0.1:${grpc_port}" WORKER_NAME=agent-smoke-worker WORKER_TASK_TYPES=agent WORKER_CAPACITY=1 \
  WORKER_METRICS_ADDR="127.0.0.1:${worker_metrics_port}" WORKER_FETCH_INTERVAL=50ms WORKER_HEARTBEAT_INTERVAL=100ms \
  WORKER_LEASE_DURATION=2s WORKER_RENEW_INTERVAL=200ms WORKER_GRACE_PERIOD=1s \
  LLM_BASE_URL="http://127.0.0.1:${provider_port}/v1" LLM_API_KEY="$provider_key" LLM_ALLOWED_MODELS=approved-model \
  LLM_REQUEST_TIMEOUT=30s LLM_MAX_OUTPUT_TOKENS=512 LLM_COST_TABLE_JSON='{"approved-model":{"prompt_microunits_per_million_tokens":1000000,"completion_microunits_per_million_tokens":1000000}}' \
  AGENT_REPOSITORIES_JSON="{\"orbit\":\"${root_dir}\"}" AGENT_MODEL=approved-model AGENT_MAX_MODEL_STEPS=4 AGENT_MAX_CONCURRENCY=1 \
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
  [[ -z "$state" || "$state" == Z* ]]
  wait "$worker_pid"
  worker_pid=""
}

create_agent_task() {
  marker=$1
  key=$2
  curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${project_token}" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${key}" \
    -d "{\"task_type\":\"agent\",\"payload\":{\"repository_root\":\"orbit\",\"issue\":\"${marker}\",\"error_log\":\"fixture\"},\"priority\":10,\"execution_timeout_ms\":30000,\"max_attempts\":3}" \
    "http://127.0.0.1:${http_port}/api/v1/tasks"
}

task_status() {
  task_id=$1
  curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${project_token}" \
    "http://127.0.0.1:${http_port}/api/v1/tasks/${task_id}" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'
}

wait_terminal() {
  task_id=$1
  expected=$2
  observed=""
  for _ in $(seq 1 400); do
    observed=$(task_status "$task_id")
    [[ "$observed" == "$expected" ]] && return 0
    [[ "$observed" == FAILED || "$observed" == CANCELED || "$observed" == SUCCEEDED ]] && break
    sleep 0.1
  done
  echo "task $task_id status=$observed, expected=$expected" >&2
  return 1
}

start_sse() {
  task_id=$1
  output=$2
  curl --silent --show-error --no-buffer --max-time 30 -H "Authorization: Bearer ${project_token}" \
    "http://127.0.0.1:${http_port}/api/v1/tasks/${task_id}/events" >"$output" 2>"${output}.err" &
  sse_pid=$!
}

stop_sse() {
  if [[ -n "$sse_pid" ]]; then
    kill -TERM "$sse_pid" 2>/dev/null || true
    wait "$sse_pid" 2>/dev/null || true
    sse_pid=""
  fi
}

wait_for_tool_trace() {
  output=$1
  for _ in $(seq 1 200); do
    started_count=$(grep -c '^event: agent_step_started' "$output" 2>/dev/null || true)
    if grep -q '^event: tool_result' "$output" 2>/dev/null && (( started_count >= 3 )); then return 0; fi
    sleep 0.05
  done
  echo "tool trace did not appear in $output" >&2
  return 1
}

start_worker

# 429 is an Orbit retry: Attempt 1 returns to PENDING, Attempt 2 reruns the
# bounded Agent loop and persists usage/cost/latency in the authoritative Result.
retry_json=$(create_agent_task agent-retry-once agent-smoke-retry)
retry_id=$(printf '%s' "$retry_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$retry_id" ]]
wait_terminal "$retry_id" SUCCEEDED
retry_result=$(curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${retry_id}/result")
[[ "$retry_result" == *'"problem_type":"repository_diagnosis"'* ]]
[[ "$retry_result" == *'"model_calls":2'* && "$retry_result" == *'"tool_calls":1'* ]]
[[ "$retry_result" == *'"total_tokens":18'* && "$retry_result" == *'"estimated_cost_microunits":18'* ]]
retry_attempts=$(curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${retry_id}/attempts")
[[ "$retry_attempts" == *'"attempt_no":1'* && "$retry_attempts" == *'"attempt_no":2'* ]]

retry_events=$(curl --silent --show-error --fail-with-body --no-buffer -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${retry_id}/events")
for event_name in task_status agent_step_started agent_step_finished tool_call tool_result final_result error; do
  grep -q "^event: ${event_name}$" <<<"$retry_events"
done
first_event_id=$(sed -n 's/^id: //p' <<<"$retry_events" | head -n 1)
[[ -n "$first_event_id" ]]
replayed=$(curl --silent --show-error --fail-with-body --no-buffer -H "Authorization: Bearer ${project_token}" -H "Last-Event-ID: ${first_event_id}" "http://127.0.0.1:${http_port}/api/v1/tasks/${retry_id}/events")
grep -q '^event: final_result$' <<<"$replayed"
grep -q '"attempt_no":2' <<<"$replayed"

# Disconnecting SSE does not cancel a Task. Explicit Task cancellation reaches
# the second model round after a completed tool call and produces CANCELED.
cancel_json=$(create_agent_task agent-slow-cancel agent-smoke-cancel)
cancel_id=$(printf '%s' "$cancel_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
start_sse "$cancel_id" "$work_dir/cancel.events"
wait_for_tool_trace "$work_dir/cancel.events"
stop_sse
[[ $(task_status "$cancel_id") == RUNNING ]]
curl --silent --show-error --fail-with-body -X POST -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${cancel_id}/cancel" >/dev/null
wait_terminal "$cancel_id" CANCELED
cancel_attempts=$(curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${cancel_id}/attempts")
[[ "$cancel_attempts" == *'"outcome":"CANCELED"'* ]]
cancel_replay=$(curl --silent --show-error --fail-with-body --no-buffer -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${cancel_id}/events")
grep -q '^event: error$' <<<"$cancel_replay"

# Crash the Worker only after a real read_file result is traced. The reaper
# expires Attempt 1; a replacement Worker completes Attempt 2.
crash_json=$(create_agent_task agent-crash-after-tool agent-smoke-crash)
crash_id=$(printf '%s' "$crash_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
start_sse "$crash_id" "$work_dir/crash.events"
wait_for_tool_trace "$work_dir/crash.events"
stop_sse
[[ $(task_status "$crash_id") == RUNNING ]]
kill -KILL "$worker_pid"
wait "$worker_pid" 2>/dev/null || true
worker_pid=""
for _ in $(seq 1 100); do
  [[ $(task_status "$crash_id") == PENDING ]] && break
  sleep 0.1
done
[[ $(task_status "$crash_id") == PENDING ]]
start_worker
wait_terminal "$crash_id" SUCCEEDED
crash_attempts=$(curl --silent --show-error --fail-with-body -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${crash_id}/attempts")
[[ "$crash_attempts" == *'"attempt_no":1'* && "$crash_attempts" == *'"attempt_no":2'* && "$crash_attempts" == *'"lease_expired":true'* ]]
crash_replay=$(curl --silent --show-error --fail-with-body --no-buffer -H "Authorization: Bearer ${project_token}" "http://127.0.0.1:${http_port}/api/v1/tasks/${crash_id}/events")
grep -q '"attempt_no":1' <<<"$crash_replay"
grep -q '"attempt_no":2' <<<"$crash_replay"
grep -q '^event: final_result$' <<<"$crash_replay"

if grep -R -q "$provider_key" "$work_dir"/*.log; then
  echo "provider API key leaked into logs" >&2
  exit 1
fi

stop_worker
echo "Agent smoke passed retry_task_id=${retry_id} cancel_task_id=${cancel_id} crash_task_id=${crash_id} sse_replay=true attempts=2"
