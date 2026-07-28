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

Health endpoints are available at `http://localhost:8080/health/live` and
`http://localhost:8080/health/ready`; Prometheus metrics are exposed at
`http://localhost:9091/metrics`.

## Development

```bash
make build
make test
make test-race
make lint
```

Architecture and execution semantics are documented under `docs/`.

