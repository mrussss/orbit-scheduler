# MySQL 8 Engineering Lab

This is an isolated Go module for reproducible MySQL 8 engineering exercises.
It is not imported by Orbit's production server, worker, relay, or consumer.

The Phase 5B.1 foundation covers versioned migrations, `BINARY(16)` UUIDs,
GORM CRUD, a native `database/sql` transaction entry point, and real MySQL 8
Testcontainers tests. Later phases own query-plan and locking experiments.

All commands in this directory use `MYSQL_LAB_DSN`; they never read Orbit's
production `DATABASE_URL`.
