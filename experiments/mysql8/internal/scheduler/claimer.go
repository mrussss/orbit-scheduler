package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
)

const MaxClaimBatch = 100

type ClaimRequest struct {
	WorkerID      uuid.UUID
	Limit         int
	LeaseDuration time.Duration
}

type ClaimedTask struct {
	ID             uuid.UUID
	AttemptNo      int
	Priority       int
	AvailableAt    time.Time
	LeaseExpiresAt time.Time
	Payload        json.RawMessage
}

type Claimer struct{ db *sql.DB }

func New(db *sql.DB) (*Claimer, error) {
	if db == nil {
		return nil, errors.New("scheduler database is required")
	}
	return &Claimer{db: db}, nil
}

func (c *Claimer) Claim(ctx context.Context, request ClaimRequest) ([]ClaimedTask, error) {
	if request.WorkerID == uuid.Nil || request.Limit <= 0 || request.Limit > MaxClaimBatch || request.LeaseDuration <= 0 {
		return nil, errors.New("invalid claim request")
	}
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT id
FROM lab_tasks
WHERE status='PENDING' AND available_at<=UTC_TIMESTAMP(6)
ORDER BY priority DESC,available_at ASC,id ASC
LIMIT ? FOR UPDATE SKIP LOCKED`, request.Limit)
	if err != nil {
		return nil, fmt.Errorf("select claim candidates: %w", err)
	}
	var ids [][]byte
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, append([]byte(nil), id...))
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	updateArgs := make([]any, 0, len(ids)+2)
	updateArgs = append(updateArgs, model.UUIDToBytes(request.WorkerID), request.LeaseDuration.Microseconds())
	for _, id := range ids {
		updateArgs = append(updateArgs, id)
	}
	updateSQL := `UPDATE lab_tasks
SET status='RUNNING',attempt_no=attempt_no+1,lease_owner=?,
    lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? MICROSECOND),
    updated_at=UTC_TIMESTAMP(6)
WHERE id IN (` + placeholders + `)`
	result, err := tx.ExecContext(ctx, updateSQL, updateArgs...)
	if err != nil {
		return nil, fmt.Errorf("update claimed tasks: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != int64(len(ids)) {
		return nil, fmt.Errorf("claim update affected=%d want=%d err=%v", affected, len(ids), err)
	}
	attemptArgs := make([]any, 0, len(ids)+1)
	attemptArgs = append(attemptArgs, model.UUIDToBytes(request.WorkerID))
	for _, id := range ids {
		attemptArgs = append(attemptArgs, id)
	}
	attemptSQL := `INSERT INTO lab_task_attempts(task_id,attempt_no,worker_id,started_at,created_at)
SELECT id,attempt_no,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)
FROM lab_tasks WHERE id IN (` + placeholders + `)`
	if _, err := tx.ExecContext(ctx, attemptSQL, attemptArgs...); err != nil {
		return nil, fmt.Errorf("insert claim attempts: %w", err)
	}
	selectArgs := make([]any, len(ids))
	for i, id := range ids {
		selectArgs[i] = id
	}
	claimedRows, err := tx.QueryContext(ctx, `SELECT id,attempt_no,priority,available_at,lease_expires_at,payload
FROM lab_tasks WHERE id IN (`+placeholders+`)
ORDER BY priority DESC,available_at ASC,id ASC`, selectArgs...)
	if err != nil {
		return nil, fmt.Errorf("read claimed tasks: %w", err)
	}
	claimed := make([]ClaimedTask, 0, len(ids))
	for claimedRows.Next() {
		var rawID []byte
		var task ClaimedTask
		if err := claimedRows.Scan(&rawID, &task.AttemptNo, &task.Priority, &task.AvailableAt, &task.LeaseExpiresAt, &task.Payload); err != nil {
			_ = claimedRows.Close()
			return nil, err
		}
		task.ID, err = model.BytesToUUID(rawID)
		if err != nil {
			_ = claimedRows.Close()
			return nil, err
		}
		claimed = append(claimed, task)
	}
	if err := claimedRows.Close(); err != nil {
		return nil, err
	}
	if err := claimedRows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) != len(ids) {
		return nil, fmt.Errorf("read %d claimed tasks, expected %d", len(claimed), len(ids))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return claimed, nil
}
