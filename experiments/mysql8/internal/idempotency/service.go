package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
)

var ErrConflict = errors.New("mysql lab idempotency conflict")

type Result struct {
	TaskID  uuid.UUID
	Created bool
}

type Service struct{ db *sql.DB }

func New(db *sql.DB) (*Service, error) {
	if db == nil {
		return nil, errors.New("idempotency database is required")
	}
	return &Service{db: db}, nil
}

func HashRequest(payload json.RawMessage) [32]byte { return sha256.Sum256(payload) }

func (s *Service) Create(ctx context.Context, task model.Task) (Result, error) {
	if task.ID.IsZero() || task.ProjectID.IsZero() || task.IdempotencyKey == nil || *task.IdempotencyKey == "" || len(task.RequestHash) != sha256.Size || !json.Valid(task.Payload) {
		return Result{}, errors.New("invalid idempotent task")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO lab_tasks(
id,project_id,idempotency_key,request_hash,status,priority,available_at,payload,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?)`, task.ID, task.ProjectID, *task.IdempotencyKey, task.RequestHash, task.Status, task.Priority, task.AvailableAt.UTC(), task.Payload, task.CreatedAt.UTC(), task.UpdatedAt.UTC())
	if err == nil {
		return Result{TaskID: task.ID.UUID(), Created: true}, nil
	}
	var mysqlErr *drivermysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
		return Result{}, err
	}
	var rawID, storedHash []byte
	if err := s.db.QueryRowContext(ctx, `SELECT id,request_hash FROM lab_tasks WHERE project_id=? AND idempotency_key=?`, task.ProjectID, *task.IdempotencyKey).Scan(&rawID, &storedHash); err != nil {
		return Result{}, fmt.Errorf("read idempotent task: %w", err)
	}
	if !bytes.Equal(storedHash, task.RequestHash) {
		return Result{}, ErrConflict
	}
	id, err := model.BytesToUUID(rawID)
	if err != nil {
		return Result{}, err
	}
	return Result{TaskID: id, Created: false}, nil
}
