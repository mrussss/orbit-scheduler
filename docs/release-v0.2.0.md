# Orbit Scheduler v0.2.0

Release verification date: 2026-08-19 (Asia/Shanghai)

This release adds the Reliable LLM Task Executor to the frozen `v0.1.0`
Phase 0–5 and MySQL Lab baseline. It does not change PostgreSQL's role as the
authoritative scheduler state.

## Final verification

The release candidate on `phase/6-llm-executor` was verified with the complete
repository gate:

```bash
make verify
```

Observed final result:

- formatting and `go vet`: passed;
- all root unit tests under the Race Detector: passed;
- every delivered root `cmd` binary: built successfully;
- PostgreSQL Testcontainers integration suite: passed in 29.191 seconds;
- Phase 5 HTTP → gRPC → two-Worker smoke: passed with one successful Attempt
  and clean Worker shutdown;
- LLM Provider, Executor, metrics, concurrency, and Worker tests under Race:
  passed;
- LLM Fake Provider smoke: passed with 429 → Attempt 2 recovery, persisted
  Usage/cost, metrics, secret-log checks, SIGTERM cancellation of an in-flight
  request, and success on a replacement Worker's Attempt 2;
- complete MySQL 8 Lab: passed in 204.858 seconds;
- MySQL deadlock, concurrent idempotency, and `SKIP LOCKED` tests repeated
  three times: passed in 226.397 seconds.

Before the final green run, aggregate verification exposed a nondeterministic
`curl | grep -q` broken-pipe exit in the LLM metrics assertion. The script was
changed to fetch each metrics document completely before matching it; the LLM
smoke passed independently and the full `make verify` then passed from the
beginning.

The environment was Go 1.22.12 on WSL2 linux/amd64 with Docker Desktop Engine
29.6.2, PostgreSQL 16 Alpine, and MySQL 8.0.46. Docker Desktop's standard WSL
socket was unavailable, so the same unmodified Make targets were invoked in a
root WSL process against Docker Desktop's mounted API proxy socket. This is an
environment access workaround, not an application or test bypass.

Timings are local verification evidence only. They are not production latency,
throughput, availability, cost, or SLA claims.

## Released scope

- `llm` Task execution through the existing Worker Registry, Lease, Attempt,
  Retry, Cancel, Fencing, Result, and graceful-shutdown path;
- OpenAI-compatible non-streaming Chat Completions Provider using server-owned
  credentials and one Provider request per Orbit Attempt;
- strict Payload decoding, model allowlist, HTTPS-by-default routing, Context
  cancellation, transport timeouts, response and normalized Result limits;
- fixed 429/5xx/network, 4xx, timeout, cancellation, malformed response, and
  oversized response classifications;
- Usage, latency, optional integer-microunit cost, stable canonical Result
  hashing, and low-cardinality LLM metrics;
- Scheduler, Worker, Reaper, task-status, and database-pool metrics added to
  the existing HTTP metrics surface;
- deterministic Fake Provider, PostgreSQL retry/fencing integration coverage,
  scheduling race coverage, and paid-API-free black-box LLM smoke;
- LLM architecture, failure/retry, security, performance, four-layer
  walkthrough, resume, and interview documentation.

## Verified boundaries

- Orbit remains at-least-once. A Provider request may execute and be billed
  before a response is lost; fencing protects the authoritative Result, not
  Provider invocation or billing exactly-once.
- API keys and Base URLs are Worker configuration, never tenant Payload or
  Result fields. Prompt and generated content are persisted by Task semantics
  but are not emitted as logs or metric labels.
- Cost is emitted only for an explicitly configured allowlisted model rate;
  absent or overflowing price data is omitted.
- The release does not implement SSE streaming, Tool Calling, RAG, MCP,
  Memory, planner or multi-agent workflows.
- Kafka Relay, Audit Consumer, DLQ, and Kafka fault recovery remain optional
  Phase 6X work and are not release claims.
- Public gRPC authentication/encryption, production deployment, SLOs, and
  broad distributed load testing remain outside this release.

## Learning checkpoint

The user explicitly requested continuation of the existing implementation,
not a second manual reimplementation. The optional `learn/6-llm-executor`
hand-copy exercise is therefore recorded as deferred; the reference code,
four-layer explanation, tests, and release evidence remain available for a
future manual exercise.
