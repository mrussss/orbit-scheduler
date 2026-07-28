package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
)

var (
	ErrWorkerNotFound   = errors.New("worker instance not found")
	ErrWorkerDraining   = errors.New("worker is draining")
	ErrStaleLease       = errors.New("stale or expired lease")
	ErrCancelRequested  = errors.New("task cancellation requested")
	ErrAlreadyFinalized = errors.New("task already finalized")
	ErrConflict         = errors.New("conflicting result")
	ErrNotFound         = errors.New("task not found")
	ErrInvalidOutcome   = errors.New("invalid outcome")
)

type FetchRequest struct {
	WorkerInstanceID uuid.UUID
	Requested        int
	LeaseDuration    time.Duration
	TraceID          string
}

type Assignment struct {
	TaskID           uuid.UUID
	ProjectID        uuid.UUID
	JobID            *uuid.UUID
	TaskType         string
	Payload          json.RawMessage
	AttemptNo        int
	LeaseExpiresAt   time.Time
	ExecutionTimeout time.Duration
	OverallDeadline  *time.Time
	TraceID          string
}

type RenewRequest struct {
	TaskID, WorkerInstanceID uuid.UUID
	AttemptNo                int
	Extension                time.Duration
}

type RenewResult struct {
	LeaseExpiresAt  time.Time
	CancelRequested bool
}

type ReportRequest struct {
	TaskID, WorkerInstanceID uuid.UUID
	AttemptNo                int
	Outcome                  domain.TaskOutcome
	Result                   json.RawMessage
	ResultHash               [32]byte
	ErrorType                domain.ErrorType
	ErrorMessage             string
	ExecutionStartedAt       time.Time
	ExecutionFinishedAt      time.Time
}

type ReportResult struct {
	Status      domain.TaskStatus
	Idempotent  bool
	AvailableAt *time.Time
}

type ReapResult struct{ Requeued, Failed, Canceled int }

type Store interface {
	FetchTasks(context.Context, FetchRequest) ([]Assignment, error)
	RenewLease(context.Context, RenewRequest) (RenewResult, error)
	ReportResult(context.Context, ReportRequest) (ReportResult, error)
	ReapExpired(context.Context, int) (ReapResult, error)
	CancelTask(context.Context, uuid.UUID, uuid.UUID) error
	CancelJob(context.Context, uuid.UUID, uuid.UUID) error
}
