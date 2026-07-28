package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type TaskStatus string

const (
	TaskPending   TaskStatus = "PENDING"
	TaskRunning   TaskStatus = "RUNNING"
	TaskSucceeded TaskStatus = "SUCCEEDED"
	TaskFailed    TaskStatus = "FAILED"
	TaskCanceled  TaskStatus = "CANCELED"
)

func (s TaskStatus) Valid() bool {
	switch s {
	case TaskPending, TaskRunning, TaskSucceeded, TaskFailed, TaskCanceled:
		return true
	default:
		return false
	}
}

func (s TaskStatus) Terminal() bool {
	return s == TaskSucceeded || s == TaskFailed || s == TaskCanceled
}

func CanTransition(from, to TaskStatus) bool {
	switch from {
	case TaskPending:
		return to == TaskRunning || to == TaskCanceled
	case TaskRunning:
		return to == TaskPending || to == TaskSucceeded || to == TaskFailed || to == TaskCanceled
	default:
		return false
	}
}

type TaskOutcome string

const (
	OutcomeSucceeded        TaskOutcome = "SUCCEEDED"
	OutcomeRetryableFailure TaskOutcome = "RETRYABLE_FAILURE"
	OutcomePermanentFailure TaskOutcome = "PERMANENT_FAILURE"
	OutcomeTimeout          TaskOutcome = "TIMEOUT"
	OutcomeCanceled         TaskOutcome = "CANCELED"
)

func (o TaskOutcome) Valid() bool {
	switch o {
	case OutcomeSucceeded, OutcomeRetryableFailure, OutcomePermanentFailure, OutcomeTimeout, OutcomeCanceled:
		return true
	default:
		return false
	}
}

type ErrorType string

const (
	ErrorNone      ErrorType = ""
	ErrorExecutor  ErrorType = "EXECUTOR"
	ErrorTimeout   ErrorType = "TIMEOUT"
	ErrorCanceled  ErrorType = "CANCELED"
	ErrorTransport ErrorType = "TRANSPORT"
	ErrorInternal  ErrorType = "INTERNAL"
)

type Task struct {
	ID                          uuid.UUID
	ProjectID                   uuid.UUID
	JobID                       *uuid.UUID
	TaskType                    string
	Payload                     json.RawMessage
	PayloadHash                 [32]byte
	Status                      TaskStatus
	Priority                    int
	AvailableAt                 time.Time
	ExecutionTimeout            time.Duration
	OverallDeadline             *time.Time
	MaxAttempts                 int
	AttemptNo                   int
	LeaseOwnerInstanceID        *uuid.UUID
	LeaseExpiresAt              *time.Time
	CancelRequestedAt           *time.Time
	Result                      json.RawMessage
	ResultHash                  []byte
	FinalErrorType              ErrorType
	FinalErrorMessage           string
	CompletedByWorkerInstanceID *uuid.UUID
	CompletedAttemptNo          *int
	CreatedAt                   time.Time
	UpdatedAt                   time.Time
	CompletedAt                 *time.Time
}

type TaskAttempt struct {
	TaskID              uuid.UUID
	AttemptNo           int
	WorkerName          string
	WorkerInstanceID    uuid.UUID
	StartedAt           time.Time
	FinishedAt          *time.Time
	Outcome             *TaskOutcome
	ErrorType           ErrorType
	ErrorMessage        string
	ExecutionDurationMS *int64
	LeaseExpired        bool
	ResultHash          []byte
}

var ErrInvalidTransition = errors.New("invalid task state transition")

func ValidateTransition(from, to TaskStatus) error {
	if !from.Valid() || !to.Valid() || !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}
