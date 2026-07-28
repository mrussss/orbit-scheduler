package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

const fetchTasksSQL = `
WITH candidates AS (
    SELECT t.id
    FROM tasks t
    WHERE t.status = 'PENDING'
      AND t.project_id = $5
      AND t.available_at <= statement_timestamp()
      AND (t.overall_deadline IS NULL OR t.overall_deadline > statement_timestamp())
      AND t.task_type = ANY($1::text[])
    ORDER BY t.priority DESC, t.available_at ASC, t.id ASC
    FOR UPDATE OF t SKIP LOCKED
    LIMIT $2
)
UPDATE tasks t
SET status = 'RUNNING',
    attempt_no = t.attempt_no + 1,
    lease_owner_instance_id = $3,
    lease_expires_at = statement_timestamp() + ($4::bigint * interval '1 millisecond'),
    updated_at = statement_timestamp()
FROM candidates c
WHERE t.id = c.id AND t.status = 'PENDING'
RETURNING t.id, t.project_id, t.job_id, t.task_type, t.payload, t.attempt_no,
          t.lease_expires_at,
          (extract(epoch FROM t.execution_timeout) * 1000)::bigint,
          t.overall_deadline`

func (s *Store) FetchTasks(ctx context.Context, request scheduler.FetchRequest) ([]scheduler.Assignment, error) {
	if request.WorkerInstanceID == uuid.Nil || request.Requested <= 0 || request.LeaseDuration <= 0 {
		return nil, fmt.Errorf("fetch tasks: invalid request")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("fetch tasks begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var advertisedTaskTypes []string
	err = tx.QueryRow(ctx, `SELECT supported_task_types FROM worker_instances WHERE id=$1`, request.WorkerInstanceID).Scan(&advertisedTaskTypes)
	if err == pgx.ErrNoRows {
		return nil, scheduler.ErrWorkerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read fetch worker types: %w", err)
	}

	var projectID uuid.UUID
	var projectCapacity, projectRunning int
	projectQuery := `
        SELECT p.id,p.max_concurrent_tasks,p.running_tasks
        FROM projects p
        JOIN LATERAL (
            SELECT t.priority,t.available_at,t.id
            FROM tasks t
            WHERE t.project_id=p.id AND t.status='PENDING'
              AND t.available_at<=statement_timestamp()
              AND (t.overall_deadline IS NULL OR t.overall_deadline>statement_timestamp())
		      AND t.task_type=ANY($1::text[])
            ORDER BY t.priority DESC,t.available_at,t.id
            LIMIT 1
        ) candidate ON true
        WHERE p.status='ACTIVE' AND p.running_tasks<p.max_concurrent_tasks
        ORDER BY candidate.priority DESC,candidate.available_at,candidate.id
        FOR UPDATE OF p %s
        LIMIT 1`
	err = tx.QueryRow(ctx, fmt.Sprintf(projectQuery, "SKIP LOCKED"), advertisedTaskTypes).Scan(&projectID, &projectCapacity, &projectRunning)
	if err == pgx.ErrNoRows {
		err = tx.QueryRow(ctx, fmt.Sprintf(projectQuery, ""), advertisedTaskTypes).Scan(&projectID, &projectCapacity, &projectRunning)
	}
	if err == pgx.ErrNoRows {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("fetch no project commit: %w", err)
		}
		return []scheduler.Assignment{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock fetch project: %w", err)
	}

	var workerName string
	var capacity, running int
	var draining bool
	var taskTypes []string
	err = tx.QueryRow(ctx, `SELECT worker_name, capacity, running_tasks, draining, supported_task_types FROM worker_instances WHERE id=$1 FOR UPDATE`, request.WorkerInstanceID).Scan(&workerName, &capacity, &running, &draining, &taskTypes)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, scheduler.ErrWorkerNotFound
		}
		return nil, fmt.Errorf("fetch worker: %w", err)
	}
	if draining {
		return nil, scheduler.ErrWorkerDraining
	}
	limit := min(request.Requested, capacity-running, projectCapacity-projectRunning, s.cfg.MaxFetchBatch)
	if limit <= 0 || len(taskTypes) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("fetch empty commit: %w", err)
		}
		return []scheduler.Assignment{}, nil
	}

	rows, err := tx.Query(ctx, fetchTasksSQL, taskTypes, limit, request.WorkerInstanceID, request.LeaseDuration.Milliseconds(), projectID)
	if err != nil {
		return nil, fmt.Errorf("fetch candidates: %w", err)
	}
	assignments := make([]scheduler.Assignment, 0, limit)
	for rows.Next() {
		var assignment scheduler.Assignment
		var timeoutMS int64
		if err := rows.Scan(&assignment.TaskID, &assignment.ProjectID, &assignment.JobID, &assignment.TaskType, &assignment.Payload, &assignment.AttemptNo, &assignment.LeaseExpiresAt, &timeoutMS, &assignment.OverallDeadline); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan assignment: %w", err)
		}
		assignment.ExecutionTimeout = time.Duration(timeoutMS) * time.Millisecond
		assignment.TraceID = request.TraceID
		assignments = append(assignments, assignment)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("fetch rows: %w", err)
	}
	rows.Close()

	batch := &pgx.Batch{}
	for _, assignment := range assignments {
		batch.Queue(`INSERT INTO task_attempts(task_id,attempt_no,worker_name,worker_instance_id,started_at,created_at,updated_at) VALUES($1,$2,$3,$4,statement_timestamp(),statement_timestamp(),statement_timestamp())`, assignment.TaskID, assignment.AttemptNo, workerName, request.WorkerInstanceID)
		payload, marshalErr := json.Marshal(map[string]any{"task_id": assignment.TaskID, "project_id": assignment.ProjectID, "attempt_no": assignment.AttemptNo, "worker_instance_id": request.WorkerInstanceID})
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal task started event: %w", marshalErr)
		}
		batch.Queue(`INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,trace_id,created_at,next_attempt_at) VALUES($1,'TASK',$2,'TASK_STARTED',1,$3,$4,$5,statement_timestamp(),statement_timestamp())`, uuid.New(), assignment.TaskID, assignment.TaskID.String(), payload, nullable(request.TraceID))
	}
	if batch.Len() > 0 {
		results := tx.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return nil, fmt.Errorf("write fetch side effects: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return nil, fmt.Errorf("close fetch batch: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE worker_instances SET running_tasks=running_tasks+$2, updated_at=statement_timestamp() WHERE id=$1`, request.WorkerInstanceID, len(assignments)); err != nil {
			return nil, fmt.Errorf("update worker running count: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE projects SET running_tasks=running_tasks+$2,updated_at=statement_timestamp() WHERE id=$1`, projectID, len(assignments)); err != nil {
			return nil, fmt.Errorf("update project running count: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("fetch tasks commit: %w", err)
	}
	return assignments, nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
