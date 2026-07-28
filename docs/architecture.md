# Architecture

Orbit separates ordinary management queries from scheduler transactions:

- Gin application services and GORM repositories handle tenant-scoped CRUD.
- pgx stores own task state transitions, leases, fencing, idempotency, and
  transactional outbox writes.
- Workers pull through gRPC and execute with bounded concurrency.
- PostgreSQL is the sole authority; Kafka publishing begins in Phase 6 and is
  intentionally outside the Phase 5 delivery.

No transaction mixes a GORM transaction with a pgx transaction.

