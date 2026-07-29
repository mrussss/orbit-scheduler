# MySQL 8 Engineering Lab

This is an isolated Go module for reproducible MySQL 8 engineering exercises.
It is not imported by Orbit's production server, worker, relay, or consumer.

The Phase 5B.1 foundation covers versioned migrations, `BINARY(16)` UUIDs,
GORM CRUD, a native `database/sql` transaction entry point, and real MySQL 8
Testcontainers tests. Later phases own query-plan and locking experiments.

All commands in this directory use `MYSQL_LAB_DSN`; they never read Orbit's
production `DATABASE_URL`.

Runtime connections deliberately reject `multiStatements=true`. The migration
runner derives a separate DSN that enables it only while applying versioned SQL.

```bash
export MYSQL_LAB_DSN='orbit:orbit@tcp(127.0.0.1:3306)/orbit_lab?parseTime=true&loc=UTC'
go run ./cmd/mysql8-lab -migrations ../../migrations/mysql8 up
go test -count=1 ./...
```

From the repository root:

```bash
make test-mysql-lab
make test-mysql-concurrency
make report-mysql-explain
```

The concurrency command repeats deadlock, `SKIP LOCKED`, and idempotency tests
three times. The explain command generates a fresh 100,000-task dataset.
