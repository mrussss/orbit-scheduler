# Architecture

## Component boundary

```mermaid
flowchart TB
    subgraph Clients
        CLI[API client]
    end

    subgraph Production[Orbit production runtime]
        Server[orbit-server\nHTTP API + gRPC scheduler]
        Worker1[orbit-worker A]
        Worker2[orbit-worker B]
        Executor1[Mock / HTTP / LLM executor]
        Executor2[Mock / HTTP / LLM executor]
        Metrics[Server + Worker Prometheus endpoints]

        Worker1 --> Executor1
        Worker2 --> Executor2
        Worker1 -->|Register / Heartbeat / Fetch\nRenew / Report| Server
        Worker2 -->|Register / Heartbeat / Fetch\nRenew / Report| Server
        Server --> Metrics
        Worker1 --> Metrics
        Worker2 --> Metrics
    end

    CLI -->|tenant-scoped HTTP| Server
    Server -->|GORM management queries| PG[(PostgreSQL 16)]
    Server -->|pgx scheduling transactions| PG
    Executor1 -->|configured HTTPS| Provider[OpenAI-compatible Provider]
    Executor2 -->|configured HTTPS| Provider

    subgraph Lab[Independent engineering lab]
        MySQLModule[experiments/mysql8\nseparate Go module]
        MySQLModule --> MySQL[(MySQL 8 / InnoDB)]
    end
```

PostgreSQL is the sole production authority. The MySQL module shares neither a
driver nor a repository abstraction with the production binaries. Kafka relay
and consumer entry points are scaffolds and are not part of the delivered data
flow.

The LLM Provider is an external execution dependency, never a state authority.
It cannot read or modify Scheduler storage. LLM requests remain inside the
Worker's existing capacity, lease, deadline, cancellation, and report flow.

## Data-access split

- Gin application services and GORM repositories handle tenant-scoped Project,
  Token, Job, Task, Attempt, and Result queries.
- pgx stores own state transitions, leases, fencing, capacity, idempotency, and
  transactional outbox writes.
- A business operation never mixes a GORM transaction and a pgx transaction.
- PostgreSQL database constraints remain the final concurrency boundary.

## Atomic fetch transaction

```mermaid
sequenceDiagram
    participant W as Worker
    participant S as Scheduler
    participant DB as PostgreSQL

    W->>S: Fetch(instance, supported types, capacity)
    S->>DB: BEGIN
    S->>DB: Lock worker and eligible project capacity
    S->>DB: SELECT tasks FOR UPDATE SKIP LOCKED
    S->>DB: Mark RUNNING and increment attempt_no
    S->>DB: Insert Attempts and Outbox events
    S->>DB: Increment authoritative running counters
    S->>DB: COMMIT
    S-->>W: Assignments + server-time lease TTL
```

If any Task, Attempt, Outbox, worker-count, or project-count write fails, the
whole transaction rolls back. Candidate ordering is stable by priority,
availability, and task ID.

## Execute, renew, and report

```mermaid
sequenceDiagram
    participant W as Worker runtime
    participant E as Executor
    participant S as Scheduler
    participant DB as PostgreSQL

    W->>E: Execute(task context + deadline)
    loop Before completion
        W->>S: Renew(task, worker, attempt)
        S->>DB: Conditional lease update
        S-->>W: renewed / cancel requested / stale
    end
    E-->>W: Outcome + result
    W->>S: Report(task, worker, attempt, outcome, hash)
    S->>DB: Conditional terminal transaction
    S-->>W: committed / idempotent replay / conflict / stale
```

`worker_instance_id + attempt_no` is the fencing identity. A worker that loses
its lease cancels local execution and cannot commit over a newer attempt.

## Lease expiry and recovery

```mermaid
sequenceDiagram
    participant A as Worker A / attempt 1
    participant R as Lease reaper
    participant DB as PostgreSQL
    participant B as Worker B / attempt 2

    A->>DB: Lease expires before completion
    R->>DB: Conditionally requeue expired RUNNING task
    B->>DB: Fetch and increment attempt_no to 2
    B->>DB: Commit attempt 2 result
    A->>DB: Late report for attempt 1
    DB-->>A: STALE_LEASE
```

This is why Orbit claims at-least-once execution rather than exactly-once
execution: the original side effect may have occurred even when its lease or
response was lost.

## Graceful shutdown

```mermaid
sequenceDiagram
    participant OS as SIGTERM
    participant W as Worker
    participant S as Scheduler
    participant T as In-flight tasks

    OS->>W: terminate
    W->>S: heartbeat DRAINING
    W->>W: stop Fetch
    W->>T: continue renew/execute/report within grace period
    alt all tasks finish
        T-->>W: drained
    else grace period expires
        W->>T: cancel task contexts
        W->>W: bounded final reporting
    end
    W->>W: stop loops and close gRPC client
```
