# Phase 5 job-ready validation

Validation date: 2026-07-28 (Asia/Shanghai)

## Black-box smoke run

Command:

```bash
make smoke-phase5
```

Observed result:

```text
phase5 smoke passed
project_id=f0e532a4-8df7-4e18-b091-dd2d39b26f46
task_id=70c23dd7-d7fb-452f-89ed-0643a418fb19
status=SUCCEEDED
attempts=1
workers=2
```

The run started PostgreSQL 16 from an empty database, applied migrations 1–9,
started the HTTP/gRPC server and two independent Worker processes, created the
Project, Token, and Task through authenticated HTTP APIs, then read the final
Result and Attempt through HTTP. The Mock task ran for 700 ms with a 500 ms
lease and 100 ms renewal interval; success on attempt 1 therefore also proves
that the real gRPC renewal path extended the lease. Both workers exited with
status 0 after SIGTERM. The script removed its container and processes.

## Regression commands

The job-ready release is accepted only after all of these pass from a clean
working tree:

```bash
make lint
make test
make build
make test-race
make test-integration
go test -tags=integration -run TestFetchEnforcesWorkerAndProjectCapacity -count=3 ./tests/integration
make smoke-phase5
```

GitHub Actions runs unit/race/build, PostgreSQL integration, and black-box smoke
as separate jobs. This release does not claim Kafka relay, consumer, DLQ, gRPC
internet exposure, or Phase 6 behavior.
