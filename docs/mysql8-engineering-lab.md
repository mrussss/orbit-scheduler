# MySQL 8 Engineering Lab

This module is an isolated engineering lab. Orbit's production server and
worker continue to use PostgreSQL exclusively.

## Reproduction

```bash
make test-mysql-foundation
make report-mysql-explain
```

The query-plan evidence below was captured on 2026-07-29 from the real
`mysql:8.0.46` Testcontainers image. The dataset generator used seed `42`, five
projects, and exactly 100,000 tasks. Project zero owns 60,100 `PENDING` tasks so
that `LIMIT 50 OFFSET 50000` is a genuine deep page; the remaining tasks are
distributed across four projects, five statuses, priorities 0 through 100, and
distinct microsecond timestamps.

These are local reproducible observations, not production latency or throughput
claims. Container startup, dataset loading, and index creation are excluded from
the individual query execution times reported by MySQL.

## Pagination queries

Offset pagination:

```sql
SELECT id, status, priority, created_at
FROM lab_tasks
WHERE project_id = ? AND status = ?
ORDER BY created_at DESC, id DESC
LIMIT 50 OFFSET 50000;
```

Cursor pagination uses the row immediately before the same logical page:

```sql
SELECT id, status, priority, created_at
FROM lab_tasks
WHERE project_id = ? AND status = ?
  AND (created_at < ? OR (created_at = ? AND id < ?))
ORDER BY created_at DESC, id DESC
LIMIT 50;
```

The integration test asserts that all 50 task IDs returned by both forms are
identical and in the same order.

## Index experiment

Each scenario drops the other page indexes, creates only the named candidate,
then runs real `EXPLAIN ANALYZE` for both queries.

| Scenario | Candidate index | Offset actual | Cursor actual | Important plan behavior |
|---|---|---:|---:|---|
| No suitable index | none | 76.6 ms | 56.1 ms | scans 60,100 project rows and sorts |
| Single column | `(status)` | 72.9 ms | 52.3 ms | optimizer still chooses project/idempotency index and sorts |
| Wrong composite order | `(status, created_at DESC, project_id, id DESC)` | 81.2 ms | 53.3 ms | project equality cannot efficiently narrow the ordered range |
| Correct composite | `(project_id, status, created_at DESC, id DESC)` | 56.8 ms | 10.8 ms | ordered index lookup; Offset still consumes 50,050 entries |
| Covering | `(project_id, status, created_at DESC, id DESC, priority)` | 13.8 ms | 0.0625 ms | index-only access; Cursor stops after 50 rows |

Representative unindexed Offset plan:

```text
Limit/Offset: 50/50000 (actual time=76.6..76.6 rows=50)
  Sort created_at DESC, id DESC (actual time=73.6..75.7 rows=50050)
    Filter status=PENDING (actual time=0.0212..50.9 rows=60100)
      Index lookup using uk_lab_task_idempotency (actual rows=60100)
```

Representative covering Cursor plan:

```text
Limit: 50 (actual time=0.0489..0.0625 rows=50)
  Filter project/status/cursor boundary (actual rows=50)
    Covering index range scan using idx_experiment
      (actual time=0.046..0.0504 rows=50)
```

The covering plan does not fetch table rows for the selected projection: MySQL
explicitly reports `Covering index lookup` or `Covering index range scan`.
Offset remains proportional to page depth because it must consume and discard
50,000 matching entries. Cursor pagination can seek to the boundary and stop
after the requested 50 rows.

## Reproducibility and interpretation

- UUIDs are derived deterministically from a fixed namespace and sequence.
- Data generation, query execution, and index changes run against real InnoDB.
- `created_at` and `id` jointly provide stable ordering.
- The migration keeps a covering page index as the current tested default.
- The scheduler fetch index remains a candidate hypothesis until its separate
  `SKIP LOCKED` workload is measured; this page experiment does not prove that
  `(status, available_at, priority DESC, id)` is optimal for claiming.
