# LLM Performance and Fault Evidence

This document records reproducible engineering observations. It is not a
production throughput, latency, cost, availability, or SLA claim.

## Executor concurrency profile

Recorded on 2026-08-18 (Asia/Shanghai):

| Item | Value |
|---|---|
| Environment | WSL2, Linux 6.18.33.2, linux/amd64 |
| CPU | Intel Core i9-14900HX, 32 logical CPUs visible |
| Memory visible | 15.5 GiB |
| Go | 1.22.12 |
| Dataset | 200 concurrent Executor calls per run |
| Fake Provider delay | 2 ms per request |
| LLM Provider concurrency | 8 |
| Worker capacity | N/A; Executor-only profile |
| Fetch batch | N/A; no Scheduler or database in this profile |
| Database pools | N/A; no database in this profile |
| Repetitions | 5 |
| Error rate | 0% in all five runs |
| Observed maximum Provider concurrency | 8 |
| Total elapsed range | 58.2–65.8 ms per 200 calls |
| P50 range | 29.5–33.3 ms |
| P95 range | 55.0–62.7 ms |
| P99 range | 57.2–65.2 ms |

The timed `go test` command, including Go tool and test-process overhead,
reported 129% aggregate CPU and 138,868 KiB maximum RSS. Those process-level
figures are not isolated Executor resource consumption.

The latency distribution is dominated by intentional semaphore queueing:
200 callers share 8 Provider slots, with a 2 ms service delay. The important
correctness observation is that all callers completed and Provider concurrency
never exceeded the configured limit.

Reproduce the profile:

```bash
go test -count=5 -run '^TestExecutorLoadProfile$' -v ./internal/executor/llm
go test -race -count=5 -run '^TestExecutorLoadProfile$' -v ./internal/executor/llm
```

## Fault evidence

The automated matrix covers:

| Fault | Evidence |
|---|---|
| 429 | retryable mapping, rate-limit metric, PostgreSQL Attempt increment test, black-box retry smoke |
| 5xx | retryable Provider and Executor tests |
| 400/401/403/404 | permanent mapping tests |
| connection failure | retryable transport test |
| client and task deadline | timeout tests |
| Context cancel | Provider and Worker propagation tests plus black-box SIGTERM cancellation/recovery |
| malformed JSON | retryable invalid-response test |
| oversized response | permanent response-limit test |
| disallowed model or oversized Prompt | rejected before Provider invocation |
| cross-origin redirect | redirect is not followed and API key is not forwarded |
| stale LLM Attempt | PostgreSQL integration test rejects conflicting Attempt 1 result after Attempt 2 succeeds |
| duplicate billing uncertainty | documented explicitly; no exactly-once claim |

## Remaining full-stack measurement

The Docker-backed LLM smoke test records the real HTTP → PostgreSQL → gRPC →
Worker → Fake Provider → Report path, but it is a correctness test rather than
a load test. Broader Scheduler batch, database-pool, multi-process, and
long-duration measurements remain optional Phase 8 work and must be reported
separately if performed.
