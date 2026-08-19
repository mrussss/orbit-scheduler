package pgstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

const maxAgentSummaryBytes = 8192

func (s *Store) RecordAgentStep(ctx context.Context, request scheduler.RecordAgentStepRequest) error {
	if err := validateAgentStep(request); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("record agent step begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var current bool
	err = tx.QueryRow(ctx, `SELECT task_type='agent' AND status='RUNNING' AND lease_owner_instance_id=$2 AND attempt_no=$3 AND lease_expires_at>statement_timestamp() FROM tasks WHERE id=$1 FOR UPDATE`, request.TaskID, request.WorkerInstanceID, request.AttemptNo).Scan(&current)
	if err == pgx.ErrNoRows {
		return scheduler.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock agent task: %w", err)
	}
	if !current {
		return scheduler.ErrStaleLease
	}
	var existingStatus scheduler.AgentStepStatus
	var existingKind scheduler.AgentStepKind
	var existingTool *string
	err = tx.QueryRow(ctx, `SELECT status,kind,tool_name FROM agent_steps WHERE task_id=$1 AND attempt_no=$2 AND step_no=$3`, request.TaskID, request.AttemptNo, request.StepNo).Scan(&existingStatus, &existingKind, &existingTool)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("read agent step: %w", err)
	}
	if err == nil {
		if existingStatus != scheduler.AgentStepRunning || existingKind != request.Kind || stringValue(existingTool) != request.ToolName || request.Status == scheduler.AgentStepRunning {
			return scheduler.ErrConflict
		}
		command, err := tx.Exec(ctx, `UPDATE agent_steps SET output_summary=$4,status=$5,finished_at=$6,updated_at=statement_timestamp() WHERE task_id=$1 AND attempt_no=$2 AND step_no=$3 AND status='RUNNING'`, request.TaskID, request.AttemptNo, request.StepNo, defaultSummary(request.OutputSummary), request.Status, request.FinishedAt)
		if err != nil {
			return fmt.Errorf("finish agent step: %w", err)
		}
		if command.RowsAffected() != 1 {
			return scheduler.ErrConflict
		}
	} else {
		var last int
		if err := tx.QueryRow(ctx, `SELECT COALESCE(max(step_no),0) FROM agent_steps WHERE task_id=$1 AND attempt_no=$2`, request.TaskID, request.AttemptNo).Scan(&last); err != nil {
			return fmt.Errorf("read last agent step: %w", err)
		}
		if request.StepNo != last+1 {
			return scheduler.ErrConflict
		}
		_, err = tx.Exec(ctx, `INSERT INTO agent_steps(task_id,attempt_no,step_no,worker_instance_id,kind,tool_name,input_summary,output_summary,status,started_at,finished_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, request.TaskID, request.AttemptNo, request.StepNo, request.WorkerInstanceID, request.Kind, nullable(request.ToolName), defaultSummary(request.InputSummary), defaultSummary(request.OutputSummary), request.Status, request.StartedAt, request.FinishedAt)
		if err != nil {
			return fmt.Errorf("insert agent step: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("record agent step commit: %w", err)
	}
	return nil
}

func validateAgentStep(request scheduler.RecordAgentStepRequest) error {
	validKind := request.Kind == scheduler.AgentStepModel || request.Kind == scheduler.AgentStepTool || request.Kind == scheduler.AgentStepFinal || request.Kind == scheduler.AgentStepError
	validStatus := request.Status == scheduler.AgentStepRunning || request.Status == scheduler.AgentStepSucceeded || request.Status == scheduler.AgentStepFailed
	if request.TaskID == uuid.Nil || request.WorkerInstanceID == uuid.Nil || request.AttemptNo <= 0 || request.StepNo <= 0 || !validKind || !validStatus || request.StartedAt.IsZero() || (request.Kind == scheduler.AgentStepTool) != (request.ToolName != "") {
		return fmt.Errorf("record agent step: invalid request")
	}
	if request.Status == scheduler.AgentStepRunning && request.FinishedAt != nil || request.Status != scheduler.AgentStepRunning && (request.FinishedAt == nil || request.FinishedAt.Before(request.StartedAt)) {
		return fmt.Errorf("record agent step: invalid timestamps")
	}
	for _, summary := range []json.RawMessage{request.InputSummary, request.OutputSummary} {
		if len(summary) > maxAgentSummaryBytes || (len(summary) > 0 && (!json.Valid(summary) || summary[0] != '{')) {
			return fmt.Errorf("record agent step: invalid summary")
		}
	}
	return nil
}

func defaultSummary(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
