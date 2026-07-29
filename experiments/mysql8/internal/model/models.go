package model

import (
	"encoding/json"
	"time"
)

type ProjectStatus string

const (
	ProjectActive   ProjectStatus = "ACTIVE"
	ProjectDisabled ProjectStatus = "DISABLED"
)

type Project struct {
	ID        BinaryUUID    `gorm:"column:id;type:binary(16);primaryKey"`
	Name      string        `gorm:"column:name;type:varchar(128);not null"`
	Status    ProjectStatus `gorm:"column:status;type:varchar(32);not null"`
	CreatedAt time.Time     `gorm:"column:created_at;precision:6;not null"`
	UpdatedAt time.Time     `gorm:"column:updated_at;precision:6;not null"`
}

func (Project) TableName() string { return "lab_projects" }

type TaskStatus string

const (
	TaskPending   TaskStatus = "PENDING"
	TaskRunning   TaskStatus = "RUNNING"
	TaskSucceeded TaskStatus = "SUCCEEDED"
	TaskFailed    TaskStatus = "FAILED"
	TaskCanceled  TaskStatus = "CANCELED"
)

type Task struct {
	ID             BinaryUUID      `gorm:"column:id;type:binary(16);primaryKey"`
	ProjectID      BinaryUUID      `gorm:"column:project_id;type:binary(16);not null"`
	IdempotencyKey *string         `gorm:"column:idempotency_key;type:varchar(128)"`
	RequestHash    []byte          `gorm:"column:request_hash;type:binary(32)"`
	Status         TaskStatus      `gorm:"column:status;type:varchar(32);not null"`
	Priority       int             `gorm:"column:priority;not null"`
	AvailableAt    time.Time       `gorm:"column:available_at;precision:6;not null"`
	AttemptNo      int             `gorm:"column:attempt_no;not null"`
	LeaseOwner     *BinaryUUID     `gorm:"column:lease_owner;type:binary(16)"`
	LeaseExpiresAt *time.Time      `gorm:"column:lease_expires_at;precision:6"`
	Payload        json.RawMessage `gorm:"column:payload;type:json;not null"`
	Result         json.RawMessage `gorm:"column:result;type:json"`
	CreatedAt      time.Time       `gorm:"column:created_at;precision:6;not null"`
	UpdatedAt      time.Time       `gorm:"column:updated_at;precision:6;not null"`
}

func (Task) TableName() string { return "lab_tasks" }

type TaskAttempt struct {
	TaskID     BinaryUUID `gorm:"column:task_id;type:binary(16);primaryKey"`
	AttemptNo  int        `gorm:"column:attempt_no;primaryKey"`
	WorkerID   BinaryUUID `gorm:"column:worker_id;type:binary(16);not null"`
	StartedAt  time.Time  `gorm:"column:started_at;precision:6;not null"`
	FinishedAt *time.Time `gorm:"column:finished_at;precision:6"`
	Outcome    *string    `gorm:"column:outcome;type:varchar(32)"`
	CreatedAt  time.Time  `gorm:"column:created_at;precision:6;not null"`
}

func (TaskAttempt) TableName() string { return "lab_task_attempts" }
