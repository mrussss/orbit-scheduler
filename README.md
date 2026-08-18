# Orbit Scheduler

[![CI](https://github.com/mrussss/orbit-scheduler/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/mrussss/orbit-scheduler/actions/workflows/ci.yml)

Orbit Scheduler is a Go and PostgreSQL distributed task execution platform. It
demonstrates atomic task claiming, lease fencing, idempotent APIs, bounded
workers, secure HTTP and reliable OpenAI-compatible LLM execution, and graceful
shutdown with reproducible integration evidence.

PostgreSQL is the only production source of truth. The repository also contains
an isolated MySQL 8 engineering lab for index, isolation, deadlock, idempotency,
and `SKIP LOCKED` experiments; it is not a second production backend.

## Why this project exists

A reliable scheduler needs more than a queue and a goroutine pool. Orbit is
built around the failure cases that make distributed execution difficult:

- two workers trying to claim the same task;
- a worker disappearing after a claim commits;
- a lease expiring while an old attempt is still running;
- a result commit succeeding while its response is lost;
- cancellation, completion, and lease reaping racing;
- shutdown beginning while work and result reporting are in flight.

Orbit provides **at-least-once execution**. A task may run more than once, but
lease ownership and a monotonic attempt number prevent an expired attempt from
overwriting the current result. Terminal task states never regress.

## Architecture

```mermaid
flowchart LR
    Client[API client] -->|HTTP / JSON| API[Orbit Server\nGin + GORM]
    API -->|tenant CRUD| PG[(PostgreSQL 16\nsource of truth)]
    WorkerA[Worker A] -->|Fetch / Renew / Report\ngRPC| Scheduler[Scheduler service\npgx transactions]
    WorkerB[Worker B] -->|Fetch / Renew / Report\ngRPC| Scheduler
    Scheduler --> PG
    WorkerA --> ExecA[Mock / HTTP / LLM executor]
    WorkerB --> ExecB[Mock / HTTP / LLM executor]
    API --> Metrics[Prometheus metrics]

    Lab[MySQL 8 engineering lab] -. isolated module .-> MySQL[(MySQL 8 / InnoDB)]
```

Management queries use GORM, while scheduling transitions use pgx and explicit
SQL transactions. The two transaction mechanisms are never mixed in one unit
of work.

The complete component and transaction diagrams are in
[`docs/architecture.md`](docs/architecture.md).

## Delivered capabilities

### Scheduler correctness

- PostgreSQL `FOR UPDATE SKIP LOCKED` task claiming with stable ordering.
- Worker capacity and project-level concurrency quotas enforced in the claim
  transaction.
- Task, Attempt, Lease, Outbox, worker count, and project count updates committed
  atomically.
- Lease renewal and result reporting fenced by task ID, worker instance ID,
  attempt number, state, and lease validity.
- Idempotent result replay for the same outcome and result hash; conflicting or
  stale results are rejected.
- Lease reaping, bounded retry backoff, and conditional cancellation without
  terminal-state regression.

### HTTP and business API

- Project, scoped API Token, Job, Task, Attempt, Result, and Cancel endpoints.
- Tenant isolation and `task:*`, `job:*`, and `project:admin` scopes.
- HMAC-SHA256 token storage, idempotency keys, and signed cursor pagination.
- Request ID, trace ID, recovery, timeout, body limit, CORS, health, and HTTP
  Prometheus metrics.

### Worker runtime and executors

- Register → Heartbeat → Fetch → Execute → Renew → Report lifecycle.
- Bounded concurrency, independent task contexts, execution deadlines, and
  bounded report retries.
- Draining and graceful shutdown with a finite grace period and client cleanup.
- Deterministic fault-capable Mock executor.
- Allowlisted HTTP executor with scheme, DNS, resolved-IP, redirect, dial-path,
  header, body, response, and proxy restrictions.
- OpenAI-compatible non-streaming LLM executor with strict payload validation,
  server-side credentials, model allowlisting, Provider concurrency, error
  classification, usage/cost accounting, and Context cancellation.
- Low-cardinality Scheduler, Worker, database pool, executor, and LLM metrics.

### Reproducible evidence

- PostgreSQL Testcontainers integration tests for atomic claims, capacity,
  rollback, idempotency, fencing, and reaping.
- A black-box smoke test that starts an empty PostgreSQL container, server, and
  two independent workers, then creates and completes a task through the real
  HTTP and gRPC paths.
- Race Detector, build, PostgreSQL integration, smoke, and isolated MySQL jobs
  in GitHub Actions.
- A Fake Provider test and black-box LLM smoke path that exercise 429 retry,
  Attempt increment, in-flight request cancellation during Worker SIGTERM,
  fenced reporting, usage/cost persistence, and secret-log checks without a
  paid model API.
- Recorded PostgreSQL and MySQL query-plan evidence without production
  performance claims.

## One-command demo

Requirements: Docker, Go 1.22+, and `curl`. Docker must be running.

```bash
make demo
```

The demo is isolated from local data. It builds the binaries, starts a temporary
PostgreSQL 16 container, applies every migration, starts the server and two
workers, creates credentials and a delayed task through HTTP, verifies lease
renewal and the final result, sends SIGTERM, requires both workers to exit
cleanly, and removes its temporary resources.

A successful run ends with output similar to:

```text
phase5 smoke passed project_id=... task_id=... status=SUCCEEDED attempts=1 workers=2
Orbit demo completed successfully.
```

## Manual local run

```bash
cp .env.example .env
make compose-up
set -a; . ./.env; set +a
make migrate-up
make run-server
```

Start a worker in another terminal after loading the same `.env`:

```bash
go run ./cmd/orbit-worker
```

Endpoints:

- API: `http://localhost:8080`
- gRPC: `localhost:9090`
- liveness: `http://localhost:8080/health/live`
- readiness: `http://localhost:8080/health/ready`
- metrics: `http://localhost:9091/metrics`
- Worker metrics when configured: `http://localhost:9092/metrics`
- Prometheus UI: `http://localhost:9095`

`ADMIN_TOKEN` creates Projects. A Project Token is returned only when created
and carries the scopes supplied in the request.

## Verification

```bash
make lint                    # go vet + formatting
make test-race               # unit tests under the Race Detector
make build                   # all root cmd binaries
make test-integration        # real PostgreSQL Testcontainers tests
make smoke-phase5            # black-box HTTP → gRPC → Worker flow
make test-llm-executor       # LLM Provider/Executor/Worker Race tests
make smoke-llm               # Fake Provider retry + SIGTERM recovery flow
make test-mysql-lab          # complete isolated MySQL 8 lab
make test-mysql-concurrency  # repeated deadlock/claim/idempotency tests
make verify                  # complete release verification
```

The MySQL tests use real InnoDB through Testcontainers and can take several
minutes. They never import a MySQL driver into Orbit's production binaries.

## Reliable LLM tasks

Add `llm` to `WORKER_TASK_TYPES` and configure the `LLM_*` values shown in
`.env.example`. Clients continue to use the generic Task API:

```json
{
  "task_type": "llm",
  "payload": {
    "model": "approved-model",
    "messages": [{"role": "user", "content": "Summarize this failure."}],
    "temperature": 0.2,
    "max_output_tokens": 500
  },
  "execution_timeout_ms": 45000,
  "max_attempts": 3
}
```

There is no chat-specific API. The normal Task, Attempt, and Result endpoints
show scheduling state and the structured model result. A network failure after
the Provider accepted a request may cause another model invocation and another
charge; Orbit fencing protects the authoritative result, not Provider billing.

The fixed baseline classification is: 429, 5xx, and transport failures are
retryable through a later Orbit Attempt; task/provider deadlines become
`TIMEOUT`; task or Worker cancellation propagates through Context; 400,
401/403, 404, and oversized responses are permanent failures. A malformed
successful response is treated as retryable. The Executor never runs an
unbounded internal retry loop.

Run `make smoke-llm` for the local Fake Provider demonstration. See
[`docs/llm-executor.md`](docs/llm-executor.md) for the full contract.

## MySQL 8 engineering lab

The lab lives in `experiments/mysql8` as an independent Go module and covers:

- versioned migrations and `BINARY(16)` UUIDs;
- GORM CRUD and native `database/sql` transactions;
- deterministic 100,000-row datasets;
- Offset versus Cursor pagination;
- single-column, incorrectly ordered composite, correct composite, and covering
  indexes with real `EXPLAIN ANALYZE` output;
- READ COMMITTED, REPEATABLE READ, and locking current reads;
- deterministic deadlock reproduction and bounded error-1213 retry;
- concurrent `FOR UPDATE SKIP LOCKED` claiming;
- unique-constraint idempotent creation.

See [`docs/mysql8-engineering-lab.md`](docs/mysql8-engineering-lab.md) and
[`docs/mysql-vs-postgresql.md`](docs/mysql-vs-postgresql.md).

## Explicit boundaries

- Kafka relay, audit consumption, and DLQ semantics are **not implemented**.
  Transactional outbox rows are written, but the relay and consumer commands
  remain reserved entry-point scaffolds.
- Worker gRPC currently uses plaintext transport without application
  authentication and must remain on a trusted private network. Internet
  exposure requires authenticated TLS or mTLS.
- HTTP executor application checks complement, but do not replace, egress
  firewalling and cloud metadata protections.
- LLM execution is non-streaming and does not provide RAG, MCP, Memory,
  multi-agent workflows, or Tool Calling. API keys and routing are process
  configuration, never task input.
- LLM messages and generated content are persisted as Task payload and Result;
  clients are responsible for submitting data compatible with their retention
  policy. Prompt and response content are not emitted as logs or metric labels.
- The recorded timings are local experiment evidence, not production latency,
  throughput, availability, or exactly-once claims.

## Documentation

- [Architecture and transaction flows](docs/architecture.md)
- [Execution semantics](docs/execution-semantics.md)
- [Task state machine](docs/state-machine.md)
- [Business API and querying](docs/business-api-and-querying.md)
- [Failure and shutdown cases](docs/failure-cases.md)
- [PostgreSQL query plans](docs/query-plans.md)
- [Phase 5 validation record](docs/job-ready-validation.md)
- [v0.1.0 release verification](docs/release-v0.1.0.md)
- [v0.2.0 release verification](docs/release-v0.2.0.md)
- [Resume wording and interview checklist](docs/resume-and-interview.md)
- [Reliable LLM Executor](docs/llm-executor.md)
- [LLM failure and retry semantics](docs/llm-failure-and-retry.md)
- [LLM security boundaries](docs/llm-security-boundaries.md)
- [LLM performance and fault evidence](docs/llm-performance-and-fault-evidence.md)

## Repository status

`v0.2.0` is the verified Phase 5 plus MySQL Lab baseline with the Reliable LLM
Executor. The complete Fake Provider, PostgreSQL integration, black-box smoke,
Race, build, and MySQL gates are recorded in the release verification document.
Kafka Relay, Audit Consumer, DLQ, Tool Calling, and broad distributed load/fault
work remain optional extensions rather than delivered claims.
