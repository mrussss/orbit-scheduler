package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type expiredTask struct {
	id              uuid.UUID
	projectID       uuid.UUID
	attemptNo       int
	owner           uuid.UUID
	cancelRequested *time.Time
	maxAttempts     int
	deadline        *time.Time
}

func (s *Store) ReapExpired(ctx context.Context, batchSize int) (scheduler.ReapResult, error) {
	if batchSize <= 0 {
		return scheduler.ReapResult{}, fmt.Errorf("reap expired: batch size must be positive")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return scheduler.ReapResult{}, fmt.Errorf("reaper begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT id,project_id,attempt_no,lease_owner_instance_id,cancel_requested_at,max_attempts,overall_deadline FROM tasks WHERE status='RUNNING' AND lease_expires_at<=statement_timestamp() ORDER BY lease_expires_at,id FOR UPDATE SKIP LOCKED LIMIT $1`, batchSize)
	if err != nil {
		return scheduler.ReapResult{}, fmt.Errorf("select expired tasks: %w", err)
	}
	candidates := make([]expiredTask, 0, batchSize)
	for rows.Next() {
		var task expiredTask
		if err := rows.Scan(&task.id, &task.projectID, &task.attemptNo, &task.owner, &task.cancelRequested, &task.maxAttempts, &task.deadline); err != nil {
			rows.Close()
			return scheduler.ReapResult{}, fmt.Errorf("scan expired task: %w", err)
		}
		candidates = append(candidates, task)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return scheduler.ReapResult{}, fmt.Errorf("iterate expired tasks: %w", err)
	}
	rows.Close()
	var result scheduler.ReapResult
	for _, task := range candidates {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT statement_timestamp()`).Scan(&now); err != nil {
			return scheduler.ReapResult{}, err
		}
		status := domain.TaskPending
		eventStatus := "REQUEUED"
		var completedAt any
		var available any
		if task.cancelRequested != nil {
			status = domain.TaskCanceled
			eventStatus = "CANCELED"
			completedAt = now
			result.Canceled++
		} else if task.attemptNo >= task.maxAttempts || (task.deadline != nil && !task.deadline.After(now)) {
			status = domain.TaskFailed
			eventStatus = "FAILED"
			completedAt = now
			result.Failed++
		} else {
			next := now.Add(s.retryDelay(task.attemptNo))
			if task.deadline != nil && !next.Before(*task.deadline) {
				status = domain.TaskFailed
				eventStatus = "FAILED"
				completedAt = now
				result.Failed++
			} else {
				available = next
				result.Requeued++
			}
		}
		command, err := tx.Exec(ctx, `UPDATE tasks SET status=$4,available_at=COALESCE($5,available_at),lease_owner_instance_id=NULL,lease_expires_at=NULL,completed_at=$6,completed_by_worker_instance_id=CASE WHEN $6::timestamptz IS NULL THEN NULL ELSE $2 END,completed_attempt_no=CASE WHEN $6::timestamptz IS NULL THEN NULL ELSE $3 END,final_error_type=CASE WHEN $4='FAILED' THEN 'TIMEOUT' ELSE NULL END,final_error_message=CASE WHEN $4='FAILED' THEN 'lease expired and retry budget exhausted' ELSE NULL END,updated_at=statement_timestamp() WHERE id=$1 AND status='RUNNING' AND lease_owner_instance_id=$2 AND attempt_no=$3 AND lease_expires_at<=statement_timestamp()`, task.id, task.owner, task.attemptNo, status, available, completedAt)
		if err != nil {
			return scheduler.ReapResult{}, fmt.Errorf("reap task %s: %w", task.id, err)
		}
		if command.RowsAffected() != 1 {
			continue
		}
		outcome := domain.OutcomeTimeout
		if status == domain.TaskCanceled {
			outcome = domain.OutcomeCanceled
		}
		_, err = tx.Exec(ctx, `UPDATE task_attempts SET finished_at=statement_timestamp(),outcome=$3,error_type='TIMEOUT',error_message='lease expired',lease_expired=true,updated_at=statement_timestamp() WHERE task_id=$1 AND attempt_no=$2 AND finished_at IS NULL`, task.id, task.attemptNo, outcome)
		if err != nil {
			return scheduler.ReapResult{}, fmt.Errorf("expire attempt: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{"task_id": task.id, "attempt_no": task.attemptNo, "decision": eventStatus})
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,created_at,next_attempt_at) VALUES($1,'TASK',$2,'TASK_LEASE_EXPIRED',1,$3,$4,statement_timestamp(),statement_timestamp())`, uuid.New(), task.id, task.id.String(), payload)
		if err != nil {
			return scheduler.ReapResult{}, fmt.Errorf("write lease expired event: %w", err)
		}
		_, err = tx.Exec(ctx, `UPDATE worker_instances SET running_tasks=GREATEST(running_tasks-1,0),updated_at=statement_timestamp() WHERE id=$1`, task.owner)
		if err != nil {
			return scheduler.ReapResult{}, fmt.Errorf("release expired worker capacity: %w", err)
		}
		_, err = tx.Exec(ctx, `UPDATE projects SET running_tasks=GREATEST(running_tasks-1,0),updated_at=statement_timestamp() WHERE id=$1`, task.projectID)
		if err != nil {
			return scheduler.ReapResult{}, fmt.Errorf("release expired project capacity: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return scheduler.ReapResult{}, fmt.Errorf("reaper commit: %w", err)
	}
	return result, nil
}

func (s *Store) CancelTask(ctx context.Context, projectID, taskID uuid.UUID) error {
	if projectID == uuid.Nil || taskID == uuid.Nil {
		return fmt.Errorf("cancel task: invalid id")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("cancel task begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status domain.TaskStatus
	var requested *time.Time
	err = tx.QueryRow(ctx, `SELECT status,cancel_requested_at FROM tasks WHERE id=$1 AND project_id=$2 FOR UPDATE`, taskID, projectID).Scan(&status, &requested)
	if err == pgx.ErrNoRows {
		return scheduler.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock cancel task: %w", err)
	}
	if status.Terminal() {
		return tx.Commit(ctx)
	}
	if requested != nil {
		return tx.Commit(ctx)
	}
	if status == domain.TaskPending {
		_, err = tx.Exec(ctx, `UPDATE tasks SET status='CANCELED',cancel_requested_at=statement_timestamp(),completed_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1 AND status='PENDING'`, taskID)
	} else {
		_, err = tx.Exec(ctx, `UPDATE tasks SET cancel_requested_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1 AND status='RUNNING'`, taskID)
	}
	if err != nil {
		return fmt.Errorf("cancel task: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "status": status})
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,created_at,next_attempt_at) VALUES($1,'TASK',$2,'TASK_CANCEL_REQUESTED',1,$3,$4,statement_timestamp(),statement_timestamp())`, uuid.New(), taskID, taskID.String(), payload)
	if err != nil {
		return fmt.Errorf("write cancel event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("cancel task commit: %w", err)
	}
	return nil
}

func (s *Store) CancelJob(ctx context.Context, projectID, jobID uuid.UUID) error {
	if projectID == uuid.Nil || jobID == uuid.Nil {
		return fmt.Errorf("cancel job: invalid id")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("cancel job begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var alreadyRequested bool
	err = tx.QueryRow(ctx, `SELECT cancel_requested_at IS NOT NULL FROM jobs WHERE id=$1 AND project_id=$2 FOR UPDATE`, jobID, projectID).Scan(&alreadyRequested)
	if err == pgx.ErrNoRows {
		return scheduler.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock cancel job: %w", err)
	}
	if !alreadyRequested {
		if _, err = tx.Exec(ctx, `UPDATE jobs SET cancel_requested_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1`, jobID); err != nil {
			return fmt.Errorf("request job cancel: %w", err)
		}
	}
	rows, err := tx.Query(ctx, `SELECT id,status,cancel_requested_at FROM tasks WHERE job_id=$1 AND project_id=$2 AND status IN ('PENDING','RUNNING') FOR UPDATE`, jobID, projectID)
	if err != nil {
		return fmt.Errorf("select job tasks: %w", err)
	}
	type cancelCandidate struct {
		id        uuid.UUID
		status    domain.TaskStatus
		requested *time.Time
	}
	var tasks []cancelCandidate
	for rows.Next() {
		var task cancelCandidate
		if err := rows.Scan(&task.id, &task.status, &task.requested); err != nil {
			rows.Close()
			return err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, task := range tasks {
		if task.requested != nil {
			continue
		}
		if task.status == domain.TaskPending {
			_, err = tx.Exec(ctx, `UPDATE tasks SET status='CANCELED',cancel_requested_at=statement_timestamp(),completed_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1 AND status='PENDING'`, task.id)
		} else {
			_, err = tx.Exec(ctx, `UPDATE tasks SET cancel_requested_at=statement_timestamp(),updated_at=statement_timestamp() WHERE id=$1 AND status='RUNNING'`, task.id)
		}
		if err != nil {
			return fmt.Errorf("cancel job task: %w", err)
		}
		payload, _ := json.Marshal(map[string]any{"task_id": task.id, "job_id": jobID})
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,created_at,next_attempt_at) VALUES($1,'TASK',$2,'TASK_CANCEL_REQUESTED',1,$3,$4,statement_timestamp(),statement_timestamp())`, uuid.New(), task.id, task.id.String(), payload); err != nil {
			return err
		}
	}
	if !alreadyRequested {
		payload, _ := json.Marshal(map[string]any{"job_id": jobID})
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,created_at,next_attempt_at) VALUES($1,'JOB',$2,'JOB_CANCEL_REQUESTED',1,$3,$4,statement_timestamp(),statement_timestamp())`, uuid.New(), jobID, jobID.String(), payload); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("cancel job commit: %w", err)
	}
	return nil
}
