package pgstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func (s *Store) RenewLease(ctx context.Context, request scheduler.RenewRequest) (scheduler.RenewResult, error) {
	if request.TaskID == uuid.Nil || request.WorkerInstanceID == uuid.Nil || request.AttemptNo <= 0 || request.Extension <= 0 {
		return scheduler.RenewResult{}, fmt.Errorf("renew lease: invalid request")
	}
	var result scheduler.RenewResult
	err := s.pool.QueryRow(ctx, `
        UPDATE tasks SET lease_expires_at=statement_timestamp()+($4::bigint*interval '1 millisecond'), updated_at=statement_timestamp()
        WHERE id=$1 AND status='RUNNING' AND lease_owner_instance_id=$2 AND attempt_no=$3
          AND lease_expires_at>statement_timestamp() AND cancel_requested_at IS NULL
        RETURNING lease_expires_at`, request.TaskID, request.WorkerInstanceID, request.AttemptNo, request.Extension.Milliseconds()).Scan(&result.LeaseExpiresAt)
	if err == nil {
		return result, nil
	}
	if err != pgx.ErrNoRows {
		return scheduler.RenewResult{}, fmt.Errorf("renew lease: %w", err)
	}
	var status domain.TaskStatus
	var owner *uuid.UUID
	var attempt int
	var expires *time.Time
	var cancelRequested *time.Time
	err = s.pool.QueryRow(ctx, `SELECT status,lease_owner_instance_id,attempt_no,lease_expires_at,cancel_requested_at FROM tasks WHERE id=$1`, request.TaskID).Scan(&status, &owner, &attempt, &expires, &cancelRequested)
	if err == pgx.ErrNoRows {
		return scheduler.RenewResult{}, scheduler.ErrNotFound
	}
	if err != nil {
		return scheduler.RenewResult{}, fmt.Errorf("classify lease renewal: %w", err)
	}
	if status.Terminal() {
		return scheduler.RenewResult{}, scheduler.ErrAlreadyFinalized
	}
	if status == domain.TaskRunning && owner != nil && *owner == request.WorkerInstanceID && attempt == request.AttemptNo && cancelRequested != nil {
		if expires != nil {
			result.LeaseExpiresAt = *expires
		}
		result.CancelRequested = true
		return result, nil
	}
	return scheduler.RenewResult{}, scheduler.ErrStaleLease
}

type taskForReport struct {
	status           domain.TaskStatus
	attemptNo        int
	owner            *uuid.UUID
	leaseExpires     *time.Time
	cancelRequested  *time.Time
	maxAttempts      int
	overallDeadline  *time.Time
	completedBy      *uuid.UUID
	completedAttempt *int
}

func (s *Store) ReportResult(ctx context.Context, request scheduler.ReportRequest) (scheduler.ReportResult, error) {
	if request.TaskID == uuid.Nil || request.WorkerInstanceID == uuid.Nil || request.AttemptNo <= 0 || !request.Outcome.Valid() {
		return scheduler.ReportResult{}, scheduler.ErrInvalidOutcome
	}
	if request.ExecutionFinishedAt.Before(request.ExecutionStartedAt) {
		return scheduler.ReportResult{}, fmt.Errorf("report result: finish precedes start")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("report result begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var task taskForReport
	var databaseNow time.Time
	err = tx.QueryRow(ctx, `SELECT status,attempt_no,lease_owner_instance_id,lease_expires_at,cancel_requested_at,max_attempts,overall_deadline,completed_by_worker_instance_id,completed_attempt_no,statement_timestamp() FROM tasks WHERE id=$1 FOR UPDATE`, request.TaskID).Scan(&task.status, &task.attemptNo, &task.owner, &task.leaseExpires, &task.cancelRequested, &task.maxAttempts, &task.overallDeadline, &task.completedBy, &task.completedAttempt, &databaseNow)
	if err == pgx.ErrNoRows {
		return scheduler.ReportResult{}, scheduler.ErrNotFound
	}
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("lock result task: %w", err)
	}
	current := task.status == domain.TaskRunning && task.owner != nil && *task.owner == request.WorkerInstanceID && task.attemptNo == request.AttemptNo && task.leaseExpires != nil && task.leaseExpires.After(databaseNow)
	if !current {
		return s.classifyRepeatedReport(ctx, tx, request, task.status)
	}

	newStatus, eventType, availableAt, decisionErr := s.resultDecision(task, request, databaseNow)
	if decisionErr != nil {
		return scheduler.ReportResult{}, decisionErr
	}
	terminal := newStatus.Terminal()
	var completedAt any
	var resultJSON any
	var resultHash any
	var finalErrorType any
	var finalErrorMessage any
	if terminal {
		completedAt = databaseNow
		resultHash = request.ResultHash[:]
		if len(request.Result) > 0 {
			resultJSON = request.Result
		}
		if request.ErrorType != domain.ErrorNone {
			finalErrorType = request.ErrorType
		}
		if request.ErrorMessage != "" {
			finalErrorMessage = request.ErrorMessage
		}
	}
	command, err := tx.Exec(ctx, `
        UPDATE tasks SET status=$5, available_at=COALESCE($6,available_at), lease_owner_instance_id=NULL,
            lease_expires_at=NULL, result=$7, result_hash=$8, final_error_type=$9, final_error_message=$10,
            completed_by_worker_instance_id=CASE WHEN $11 THEN $2 ELSE NULL END,
            completed_attempt_no=CASE WHEN $11 THEN $3 ELSE NULL END,
            completed_at=$12, updated_at=statement_timestamp()
        WHERE id=$1 AND status='RUNNING' AND lease_owner_instance_id=$2 AND attempt_no=$3
          AND lease_expires_at>statement_timestamp()`, request.TaskID, request.WorkerInstanceID, request.AttemptNo, request.Outcome, newStatus, availableAt, resultJSON, resultHash, finalErrorType, finalErrorMessage, terminal, completedAt)
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("update result task: %w", err)
	}
	if command.RowsAffected() != 1 {
		return scheduler.ReportResult{}, scheduler.ErrStaleLease
	}
	duration := request.ExecutionFinishedAt.Sub(request.ExecutionStartedAt).Milliseconds()
	if duration < 0 {
		duration = 0
	}
	command, err = tx.Exec(ctx, `UPDATE task_attempts SET finished_at=$4,outcome=$5,error_type=$6,error_message=$7,execution_duration_ms=$8,result_hash=$9,updated_at=statement_timestamp() WHERE task_id=$1 AND attempt_no=$2 AND worker_instance_id=$3 AND finished_at IS NULL`, request.TaskID, request.AttemptNo, request.WorkerInstanceID, request.ExecutionFinishedAt, request.Outcome, nullable(string(request.ErrorType)), nullable(request.ErrorMessage), duration, request.ResultHash[:])
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("update result attempt: %w", err)
	}
	if command.RowsAffected() != 1 {
		return scheduler.ReportResult{}, fmt.Errorf("update result attempt: %w", scheduler.ErrConflict)
	}
	payload, err := json.Marshal(map[string]any{"task_id": request.TaskID, "attempt_no": request.AttemptNo, "worker_instance_id": request.WorkerInstanceID, "outcome": request.Outcome, "status": newStatus})
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("marshal result event: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,created_at,next_attempt_at) VALUES($1,'TASK',$2,$3,1,$4,$5,statement_timestamp(),statement_timestamp())`, uuid.New(), request.TaskID, eventType, request.TaskID.String(), payload)
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("write result event: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE worker_instances SET running_tasks=GREATEST(running_tasks-1,0),updated_at=statement_timestamp() WHERE id=$1`, request.WorkerInstanceID)
	if err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("release worker capacity: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return scheduler.ReportResult{}, fmt.Errorf("report result commit: %w", err)
	}
	return scheduler.ReportResult{Status: newStatus, AvailableAt: availableAt}, nil
}

func (s *Store) classifyRepeatedReport(ctx context.Context, tx pgx.Tx, request scheduler.ReportRequest, status domain.TaskStatus) (scheduler.ReportResult, error) {
	var workerID uuid.UUID
	var outcome *domain.TaskOutcome
	var hash []byte
	var finished *time.Time
	err := tx.QueryRow(ctx, `SELECT worker_instance_id,outcome,result_hash,finished_at FROM task_attempts WHERE task_id=$1 AND attempt_no=$2`, request.TaskID, request.AttemptNo).Scan(&workerID, &outcome, &hash, &finished)
	if err != nil && err != pgx.ErrNoRows {
		return scheduler.ReportResult{}, fmt.Errorf("classify repeated result: %w", err)
	}
	if err == nil && workerID == request.WorkerInstanceID && finished != nil {
		if outcome != nil && *outcome == request.Outcome && bytes.Equal(hash, request.ResultHash[:]) {
			if err := tx.Commit(ctx); err != nil {
				return scheduler.ReportResult{}, fmt.Errorf("commit repeated result: %w", err)
			}
			return scheduler.ReportResult{Status: status, Idempotent: true}, nil
		}
		return scheduler.ReportResult{}, scheduler.ErrConflict
	}
	if status.Terminal() {
		return scheduler.ReportResult{}, scheduler.ErrAlreadyFinalized
	}
	return scheduler.ReportResult{}, scheduler.ErrStaleLease
}

func (s *Store) resultDecision(task taskForReport, request scheduler.ReportRequest, now time.Time) (domain.TaskStatus, string, *time.Time, error) {
	switch request.Outcome {
	case domain.OutcomeSucceeded:
		return domain.TaskSucceeded, "TASK_SUCCEEDED", nil, nil
	case domain.OutcomePermanentFailure:
		return domain.TaskFailed, "TASK_FAILED", nil, nil
	case domain.OutcomeCanceled:
		if task.cancelRequested == nil {
			return "", "", nil, scheduler.ErrInvalidOutcome
		}
		return domain.TaskCanceled, "TASK_CANCELED", nil, nil
	case domain.OutcomeRetryableFailure, domain.OutcomeTimeout:
		if task.cancelRequested != nil {
			return domain.TaskCanceled, "TASK_CANCELED", nil, nil
		}
		if task.attemptNo >= task.maxAttempts || (task.overallDeadline != nil && !task.overallDeadline.After(now)) {
			return domain.TaskFailed, "TASK_FAILED", nil, nil
		}
		delay := s.retryDelay(task.attemptNo)
		available := now.Add(delay)
		if task.overallDeadline != nil && !available.Before(*task.overallDeadline) {
			return domain.TaskFailed, "TASK_FAILED", nil, nil
		}
		return domain.TaskPending, "TASK_RETRY_SCHEDULED", &available, nil
	default:
		return "", "", nil, scheduler.ErrInvalidOutcome
	}
}

func (s *Store) retryDelay(attempt int) time.Duration {
	s.randomMu.Lock()
	defer s.randomMu.Unlock()
	return domain.RetryDelay(attempt, s.cfg.RetryBase, s.cfg.RetryMax, s.random)
}
