# Orbit Scheduler

Orbit Scheduler is a Go/PostgreSQL distributed task execution platform with
lease fencing, idempotent APIs, bounded workers, and transactional lifecycle
events. PostgreSQL is the only production source of truth.

The implementation follows the staged specification in `develop.md`. The
current delivery target ends at Phase 5: schema, scheduler core, business API,
gRPC worker runtime, executors, and graceful shutdown.

## Local quick start

```bash
cp .env.example .env
make compose-up
set -a; . ./.env; set +a
make migrate-up
make run-server
```

`ADMIN_TOKEN` bootstraps projects. Project tokens are stored only as
peppered hashes and carry `task:*`, `job:*`, and `project:admin` scopes.
Start a worker in another terminal with `go run ./cmd/orbit-worker`; every
process start creates a fresh worker instance UUID.

Health endpoints are available at `http://localhost:8080/health/live` and
`http://localhost:8080/health/ready`; Prometheus metrics are exposed at
`http://localhost:9091/metrics`.

## Development

```bash
make build
make test
make test-race
make test-integration
make smoke-phase5
make lint
```

Architecture and execution semantics are documented under `docs/`.

The Phase 5 smoke test starts an isolated PostgreSQL container, applies every
migration, launches the API/gRPC server and two workers, creates credentials
and a delayed Mock task through HTTP, verifies renewal and the final result,
then sends SIGTERM and requires both workers to exit cleanly.

The worker gRPC endpoint currently uses plaintext transport without application
authentication. It is intended only for a trusted private network in this
release; internet exposure requires an authenticated TLS or mTLS boundary.

## Delivered scope

- PostgreSQL migrations, constraints, indexes, and split GORM/pgx pools
- lease-fenced Fetch/Renew/Report/Reaper transactions with worker and project capacity limits
- tenant-safe Project, Token, Job, Task, Attempt, Result, and Cancel APIs
- generated gRPC protocol and bounded Fetch → Execute → Renew → Report runtime
- deterministic fault-capable Mock executor
- allowlisted HTTP executor with DNS/IP/redirect SSRF checks and size limits
- execution deadlines, draining, bounded reporting, and graceful shutdown

Transactional outbox rows are written in the delivered phases. Kafka relay,
consumer, and DLQ behavior belong to Phase 6 and are intentionally not
implemented here.

The recorded job-ready acceptance run is in
[`docs/job-ready-validation.md`](docs/job-ready-validation.md).
