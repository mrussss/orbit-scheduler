# Orbit Scheduler v0.1.0

Release verification date: 2026-07-29 (Asia/Shanghai)

This is the first feature-frozen portfolio release. It contains the complete
Phase 5 production path and the isolated MySQL 8 job-ready engineering lab.

## Final verification

The release candidate was verified from `main` with:

```bash
make demo
make verify
```

Observed final result:

- formatting and `go vet`: passed;
- root unit tests under the Race Detector: passed;
- every root `cmd` binary: built successfully;
- PostgreSQL Testcontainers integration suite: passed in 10.367 seconds;
- isolated HTTP → gRPC → two-Worker smoke flow: passed with one successful
  attempt and graceful shutdown;
- complete MySQL 8 lab: passed in 188.421 seconds;
- deadlock, concurrent idempotency, and `SKIP LOCKED` tests repeated three
  times: passed in 215.321 seconds.

During the release run, repeated concurrency testing exposed that one empty
MySQL `SKIP LOCKED` result had been treated as global completion. The test was
corrected to recognize that other transactions may temporarily lock every
remaining candidate, the isolated scenario passed five consecutive runs, and
the complete release verification then passed from the beginning.

Timings above describe this local validation run only. They are not production
latency or throughput claims.

## Released scope

- tenant-safe HTTP business API and PostgreSQL migrations;
- atomic PostgreSQL Fetch, Renew, Report, Reaper, cancel, capacity, fencing,
  idempotency, and transactional outbox writes;
- gRPC Worker runtime with bounded execution and graceful shutdown;
- fault-capable Mock executor and guarded HTTP executor;
- health checks, structured logging, HTTP Prometheus metrics, CI, Race,
  PostgreSQL integration, and black-box smoke tests;
- independent MySQL 8 module with migrations, CRUD, query plans, isolation,
  deadlock retry, unique-constraint idempotency, and simplified concurrent
  claiming evidence;
- architecture, semantics, database evidence, demo, and interview handoff
  documentation.

## Frozen boundaries

The release intentionally does not claim:

- Kafka relay, Audit Consumer, Consumer Group, or DLQ implementation;
- exactly-once execution;
- authenticated or encrypted public gRPC transport;
- production deployment, SLOs, complete service metrics, distributed tracing,
  or a broad load/fault campaign;
- MySQL as an Orbit production backend.

The repository is suitable for demonstration and continued study in this
defined scope. New feature phases are deferred; future changes should begin
from a separately justified requirement rather than extending this release by
default.
