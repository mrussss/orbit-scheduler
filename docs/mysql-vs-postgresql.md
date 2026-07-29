# MySQL 8 and PostgreSQL in Orbit

Orbit uses PostgreSQL as its only production system of record. The MySQL 8
module is an isolated engineering lab, not a second scheduler implementation or
a runtime database option.

## Concrete implementation differences

| Area | Orbit PostgreSQL production path | MySQL 8 lab |
|---|---|---|
| Go access | pgx plus production GORM repositories | `database/sql`, MySQL Driver, and isolated GORM CRUD |
| UUID | native `uuid` | `BINARY(16)` with explicit scan/value conversion |
| JSON | `jsonb` | `JSON` |
| Claim update | CTE candidate selection followed by `UPDATE ... RETURNING` | lock IDs, `UPDATE ... IN (...)`, insert attempts, then reread |
| Claim clock | `statement_timestamp()` | `UTC_TIMESTAMP(6)` |
| Claim isolation | explicit READ COMMITTED | explicit READ COMMITTED |
| Concurrent claim | `FOR UPDATE OF t SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` |
| Pagination evidence | PostgreSQL plans documented separately | MySQL `EXPLAIN ANALYZE` over 100,000 deterministic tasks |
| Idempotency | PostgreSQL production transaction and unique constraints | unique `(project_id,idempotency_key)` plus request-hash comparison |
| Side effects | Task, Attempt, Outbox, worker and project counters in one transaction | simplified Task and Attempt transaction only |

PostgreSQL's `RETURNING` lets the production fetch path update and obtain the
assignments in one statement. MySQL 8 does not offer the same general
`UPDATE ... RETURNING` shape, so the lab holds row locks while it performs a
select/update/insert/select sequence in one transaction.

## Isolation and locking

PostgreSQL and MySQL both support MVCC and `SKIP LOCKED`, but their defaults and
lock details are not interchangeable:

- PostgreSQL commonly runs READ COMMITTED by default; InnoDB defaults to
  REPEATABLE READ.
- InnoDB locking reads can acquire record, gap, or next-key locks depending on
  predicates and indexes. The lab explicitly uses READ COMMITTED for claiming
  to keep its intended lock scope clear.
- An InnoDB ordinary RR read uses a consistent snapshot, while `FOR UPDATE` is a
  current locking read. The lab verifies this distinction with two connections.
- Both systems can abort a deadlock victim. Application code must retry the
  complete transaction, never continue using the aborted transaction.

`SKIP LOCKED` improves competing-consumer throughput, but it does not provide
fairness by itself. Stable ordering helps, yet frequently locked rows can still
be bypassed. Correctness therefore depends on state predicates, attempts, and
lease fencing rather than selection order alone.

## Index and pagination interpretation

Both optimizers are cost based. A plausible composite index is only a hypothesis
until tested against representative cardinality and predicates. In the MySQL
lab, a covering `(project_id,status,created_at DESC,id DESC,priority)` index made
the projection index-only and allowed Cursor pagination to stop after 50 rows.
Deep Offset still consumed 50,050 entries.

The result is evidence for this deterministic local dataset, not a universal
claim that MySQL, PostgreSQL, Cursor, or any one index is always faster. Changes
to tenant skew, status selectivity, due-task ratio, projection, and page size can
change the chosen plan.

## Selection rationale

PostgreSQL remains the appropriate production choice for this repository because
the existing scheduler is built and tested around its transactional CTEs,
`RETURNING`, native UUID/array/interval types, outbox transaction, quota locking,
lease fencing, and operational tooling. Introducing database switching or dual
writes would duplicate correctness-critical scheduler semantics without a
product requirement.

The MySQL lab is valuable for a different reason: it demonstrates real InnoDB
schema design, connection handling, query plans, isolation behavior, deadlock
retry, unique-key idempotency, and concurrent claiming while keeping production
architecture honest and comprehensible.
