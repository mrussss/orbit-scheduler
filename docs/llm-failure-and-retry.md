# LLM Failure and Retry Semantics

Orbit still provides at-least-once execution for LLM tasks. Fencing protects
the authoritative task result; it cannot make an external model invocation
exactly once.

## Error mapping

| Provider condition | Orbit outcome | Retry owner |
|---|---|---|
| HTTP 429 | `RETRYABLE_FAILURE` | Scheduler Attempt backoff |
| HTTP 5xx | `RETRYABLE_FAILURE` | Scheduler Attempt backoff |
| transport or temporary network error | `RETRYABLE_FAILURE` | Scheduler Attempt backoff |
| execution deadline or HTTP timeout | `TIMEOUT` | Scheduler policy |
| task or Worker shutdown cancellation | `CANCELED` inside Executor | Worker converts shutdown interruption to retryable before reporting |
| HTTP 400 | `PERMANENT_FAILURE` | none |
| HTTP 401/403 | `PERMANENT_FAILURE` | operator must fix configuration |
| HTTP 404 | `PERMANENT_FAILURE` | operator must fix model configuration |
| malformed successful response | `RETRYABLE_FAILURE` | Scheduler Attempt backoff |
| response body over limit | `PERMANENT_FAILURE` | none |

Provider error strings expose only a bounded category and status code. Response
bodies are not copied into task errors.

## Request accepted but response lost

The model Provider may process and bill a request before the Worker observes a
network failure. Orbit then records a retryable failure and may call the model
again in a later Attempt. This can produce different text and more than one
charge.

Orbit guarantees only that:

- every accepted result is checked against task ID, Worker instance ID,
  Attempt number, current state, and lease validity;
- an expired Attempt cannot overwrite a newer result;
- repeating an already accepted report with the same outcome and hash is
  idempotent;
- terminal task state does not regress.

It does not guarantee Provider-side request idempotency, deterministic output,
or exactly-once billing.

## Shutdown

Graceful shutdown stops Fetch first and waits through the configured grace
period. If an LLM request is still active when the period ends, the Runtime
cancels the task Context. The HTTP request observes that Context. The
interrupted execution is reported as retryable where the lease remains valid;
otherwise its result is discarded and the Lease Reaper recovers the task.

## Test evidence

Unit and Runtime tests cover:

- 429, 5xx, 400, 401, and 404 mapping;
- malformed and oversized responses;
- Context cancellation and deadline propagation;
- Provider concurrency bounds;
- Worker shutdown cancellation and retryable reporting;
- deterministic result hashing and integer cost calculation.

The PostgreSQL integration suite additionally covers 429 followed by Attempt 2
success and rejection of a conflicting result from Attempt 1. The black-box
LLM smoke test exercises the same retry through the real HTTP, gRPC, Worker,
and PostgreSQL path.
