package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type ProjectStatus string

const (
	ProjectActive   ProjectStatus = "ACTIVE"
	ProjectDisabled ProjectStatus = "DISABLED"
)

type Project struct {
	ID                   uuid.UUID
	Name                 string
	Status               ProjectStatus
	TaskQuota            int64
	MaxConcurrentTasks   int
	RunningTasks         int
	CreatedAt, UpdatedAt time.Time
}
type APIToken struct {
	ID, ProjectID         uuid.UUID
	TokenPrefix           string
	TokenHash             []byte
	Name                  string
	Scopes                []string
	Disabled              bool
	ExpiresAt, LastUsedAt *time.Time
	CreatedAt, UpdatedAt  time.Time
}
type Job struct {
	ID, ProjectID        uuid.UUID
	Name                 string
	CancelRequestedAt    *time.Time
	Metadata             json.RawMessage
	CreatedAt, UpdatedAt time.Time
}
type WorkerInstance struct {
	ID                         uuid.UUID
	WorkerName, Hostname       string
	Capacity                   int
	SupportedTaskTypes         []string
	RunningTasks               int
	ReportedRunningTasks       int
	Draining                   bool
	LastHeartbeatAt, StartedAt time.Time
	ProcessVersion             string
	Metadata                   json.RawMessage
}
type OutboxEvent struct {
	EventID       uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	EventVersion  int
	EventKey      string
	Payload       json.RawMessage
	TraceID       string
	CreatedAt     time.Time
}
type AuditEvent struct {
	EventID                uuid.UUID
	AggregateType          string
	AggregateID            uuid.UUID
	EventType              string
	EventVersion           int
	Payload                json.RawMessage
	KafkaTopic             string
	KafkaPartition         int
	KafkaOffset            int64
	OccurredAt, ConsumedAt time.Time
}

type JobDerivedStatus string

const (
	JobPending   JobDerivedStatus = "PENDING"
	JobRunning   JobDerivedStatus = "RUNNING"
	JobSucceeded JobDerivedStatus = "SUCCEEDED"
	JobFailed    JobDerivedStatus = "FAILED"
	JobCanceled  JobDerivedStatus = "CANCELED"
	JobPartial   JobDerivedStatus = "PARTIAL"
)

type JobCounts struct{ Total, Pending, Running, Succeeded, Failed, Canceled int64 }

func DeriveJobStatus(c JobCounts) JobDerivedStatus {
	if c.Total == 0 || c.Pending == c.Total {
		return JobPending
	}
	if c.Running > 0 || c.Pending > 0 {
		return JobRunning
	}
	if c.Succeeded == c.Total {
		return JobSucceeded
	}
	if c.Failed == c.Total {
		return JobFailed
	}
	if c.Canceled == c.Total {
		return JobCanceled
	}
	return JobPartial
}
