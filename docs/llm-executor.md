# Reliable LLM Executor

The `llm` task type runs a non-streaming OpenAI-compatible Chat Completion
inside the existing Orbit Worker. It is an executor, not a second scheduler or
an independent AI service.

The Provider maps Orbit's `max_output_tokens` field to the current Chat
Completions `max_completion_tokens` request field and sends `stream=false`.
This follows the [official OpenAI Chat Completions reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
while keeping Orbit's task contract Provider-neutral.

```text
Create Task(task_type=llm)
  -> PostgreSQL authoritative task state
  -> Worker Fetch / lease / attempt
  -> llm.Executor
  -> OpenAI-compatible POST /v1/chat/completions
  -> usage, latency, and optional estimated cost result
  -> fenced ReportResult
```

The path inherits Worker capacity, execution deadlines, lease renewal, attempt
backoff, stale-result fencing, cancellation, and graceful shutdown. The
Provider and Executor never access the scheduler store directly.

## Four-layer walkthrough

### 1. Business call chain

The tenant creates a normal Task with `task_type=llm`. The API persists it;
an `llm` Worker fetches it over gRPC; the Registry selects `llm.Executor`;
the Executor validates the payload and calls the configured Provider once;
the Runtime reports the normalized result through the same fenced
`ReportResult` method used by every other Executor. Task, Attempt, and Result
queries need no chat-specific API.

### 2. Data and state changes

Creation stores the client-authored messages in the Task payload with status
`PENDING`. Fetch atomically changes it to `RUNNING`, increments `attempt_no`,
creates a Task Attempt, and assigns a Lease. A retryable Provider outcome
finishes that Attempt and returns the Task to `PENDING` with a later
`available_at`; a successful outcome stores only the normalized Result and
changes the Task to `SUCCEEDED`. Result updates are conditional on task ID,
Worker instance, Attempt number, state, and Lease, so an older LLM response
cannot overwrite a later Attempt.

### 3. Concurrency and failure behavior

Worker capacity bounds all execution, while the LLM semaphore independently
bounds Provider calls. Lease renewal and execution share cancellation through
Context. A task deadline maps to `TIMEOUT`; an explicit cancel maps to
`CANCELED`; forced Worker shutdown converts the interrupted execution to a
retryable report when the Lease is still valid. A request that reached the
Provider may still be billed even when its response is lost, so a later
Attempt can be a duplicate external invocation despite correct fencing.

### 4. File map

- `internal/executor/llm/payload.go`: untrusted payload contract and limits.
- `internal/executor/llm/openai_compatible.go`: authenticated one-request HTTP
  Provider, protocol parsing, timeouts, body limit, and status classification.
- `internal/executor/llm/executor.go`: semaphore, Outcome mapping, normalized
  Result, cost, size check, canonical hash, and observer calls.
- `internal/executor/llm/metrics.go`: bounded Prometheus collectors.
- `cmd/orbit-worker/main.go`: configuration wiring and Registry registration.
- `internal/worker/runtime.go`: lease renewal, task deadline, cancellation,
  shutdown conversion, and fenced result reporting.
- `scripts/fake_llm_provider.go` and `scripts/smoke_llm.sh`: deterministic
  Provider faults and the paid-API-free black-box demonstration.
- `tests/integration/llm_test.go`: Attempt retry, secret persistence check, and
  stale-result fencing against PostgreSQL.

## Payload contract

```json
{
  "model": "approved-model",
  "messages": [
    {"role": "system", "content": "Return concise JSON."},
    {"role": "user", "content": "Analyze this failure."}
  ],
  "temperature": 0.2,
  "max_output_tokens": 800,
  "response_format": "json_object",
  "metadata": {"prompt_version": "diagnose-v1"}
}
```

The decoder rejects unknown fields. Consequently a task cannot supply an API
key, Base URL, arbitrary HTTP header, or Authorization value. Models must be in
`LLM_ALLOWED_MODELS`; roles are limited to `system`, `user`, and `assistant` in
the baseline executor. Message count, individual content, total prompt bytes,
temperature, metadata, and output tokens are bounded before a Provider request
is made.

## Result contract

```json
{
  "provider": "openai-compatible",
  "model": "approved-model",
  "content": "...",
  "finish_reason": "stop",
  "usage": {
    "prompt_tokens": 120,
    "completion_tokens": 240,
    "total_tokens": 360
  },
  "latency_ms": 842,
  "estimated_cost_microunits": 315
}
```

The result is canonicalized before its SHA-256 result hash is computed. Cost is
present only when the requested model has an explicit cost-table entry. It is
calculated with integer arithmetic:

```text
prompt_tokens * prompt_rate_microunits_per_million / 1,000,000
+ completion_tokens * completion_rate_microunits_per_million / 1,000,000
```

Missing or overflowing cost data is omitted; Orbit does not invent a price.
The canonical normalized result is also checked against Orbit's 1 MiB report
limit before it reaches gRPC; an oversized result is a permanent failure.

## Configuration

Enable the executor by adding `llm` to `WORKER_TASK_TYPES`, then configure:

```text
LLM_PROVIDER=openai-compatible
LLM_BASE_URL=https://provider.example/v1
LLM_API_KEY=...
LLM_ALLOWED_MODELS=approved-model
LLM_REQUEST_TIMEOUT=45s
LLM_DIAL_TIMEOUT=5s
LLM_TLS_HANDSHAKE_TIMEOUT=5s
LLM_MAX_PROMPT_BYTES=262144
LLM_MAX_RESPONSE_BYTES=1048576
LLM_MAX_OUTPUT_TOKENS=4096
LLM_MAX_CONCURRENCY=4
LLM_COST_TABLE_JSON={}
```

LLM configuration is mandatory only for Workers advertising the `llm` task
type. Production mode requires HTTPS. Local test mode permits HTTP so the Fake
Provider can use a loopback listener.

## Concurrency and retry ownership

Worker capacity limits total tasks in this process. `LLM_MAX_CONCURRENCY` adds
a Provider-specific semaphore, so slow requests cannot create unbounded
goroutines or connections. The HTTP transport pools idle connections and has
independent dial, TLS, request, and response-size limits.

The Provider performs one HTTP request. It does not perform business retries.
Retryable failures are reported to Orbit, which releases the lease and creates
a later, incremented Attempt. This avoids multiplying Provider retries by
Scheduler retries.

## Metrics

The Worker metrics endpoint exports:

```text
orbit_llm_requests_total
orbit_llm_request_duration_seconds
orbit_llm_tokens_total
orbit_llm_estimated_cost_microunits_total
orbit_llm_rate_limited_total
orbit_llm_in_flight
```

Provider and model labels come from configuration. Task IDs, Project IDs,
prompts, responses, and Provider request IDs are never labels.

## Verification

```bash
make test-llm-executor
make smoke-llm
```

The first command runs Provider, Executor, metrics, concurrency, and Worker
shutdown tests under the Race Detector. The smoke command uses a local Fake
Provider and temporary PostgreSQL container to verify HTTP task creation,
gRPC fetch/report, one 429 retry, Attempt increment, usage/cost persistence,
metrics, secret-log checks, and SIGTERM cancellation of an in-flight Provider
request followed by recovery on a new Worker without calling a paid API.
