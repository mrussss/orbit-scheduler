package command

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
)

type CreateTask struct {
	ProjectID               uuid.UUID
	JobID                   *uuid.UUID
	TaskType                string
	Payload                 json.RawMessage
	Priority                int
	AvailableAt             time.Time
	ExecutionTimeout        time.Duration
	OverallDeadline         *time.Time
	MaxAttempts             int
	IdempotencyKey, TraceID string
}
type CreatedTask struct {
	Task    domain.Task
	Created bool
}
type CreateJob struct {
	ProjectID               uuid.UUID
	Name                    string
	Metadata                json.RawMessage
	Tasks                   []CreateTask
	IdempotencyKey, TraceID string
}
type CreatedJob struct {
	Job     domain.Job
	Tasks   []domain.Task
	Created bool
}
type Creator interface {
	CreateTask(context.Context, CreateTask) (CreatedTask, error)
	CreateJob(context.Context, CreateJob) (CreatedJob, error)
}
