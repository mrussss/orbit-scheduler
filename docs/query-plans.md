# PostgreSQL query-plan record

Recorded on 2026-07-28 with PostgreSQL 16 Alpine in Docker Desktop, 100,001
task rows, warm local buffers, and `ANALYZE` immediately before measurement.
Numbers are development evidence, not production benchmark claims.

## Tenant status cursor page

```sql
SELECT id, status, created_at
FROM tasks
WHERE project_id = $1 AND status = 'PENDING'
ORDER BY created_at DESC, id DESC
LIMIT 50;
```

Plan: `Index Only Scan using tasks_project_status_page_idx`; 50 rows,
4 shared-buffer hits, 0.051 ms execution. This matches the equality prefix and
both descending cursor columns.

## Fetch candidates

The first 10,001-row experiment had almost every row matching one task type.
PostgreSQL correctly preferred a sequential scan plus sort (4.803 ms) over an
unselective index. After adding realistic type skew and measuring at 100,001
rows, the query used `tasks_fetch_type_order_idx`:

```text
Index Scan using tasks_fetch_type_order_idx
Index Cond: task_type = ? AND status = 'PENDING'
            AND available_at <= statement_timestamp()
Buffers: shared hit=103 (203 including row locks)
Execution Time: 0.134 ms for LIMIT 100
```

This experiment is why the migration includes
`(task_type, status, priority DESC, available_at, id)` in addition to the more
general scheduler and availability indexes. The extra write/storage cost is
accepted because this is the scheduler's hottest read/lock path.

## Expired leases

`tasks_expired_lease_idx` produced an index scan with incremental sort for the
empty expired set: one shared-buffer hit and 0.045 ms. The partial predicate
keeps terminal and pending rows out of this housekeeping index.

Reproduce by loading representative task-type distributions, running
`ANALYZE tasks`, then using `EXPLAIN (ANALYZE, BUFFERS)`; results with tiny or
100%-matching data distributions are not comparable.
