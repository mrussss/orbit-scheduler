package store

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
)

type Page struct {
	Limit         int
	CreatedBefore time.Time
	IDBefore      uuid.UUID
}

type ProjectRepository interface {
	Create(context.Context, domain.Project) (domain.Project, error)
	Get(context.Context, uuid.UUID) (domain.Project, error)
	Update(context.Context, domain.Project) (domain.Project, error)
}

type TokenRepository interface {
	Create(context.Context, domain.APIToken) (domain.APIToken, error)
	FindActiveByPrefix(context.Context, string) ([]domain.APIToken, error)
	Disable(context.Context, uuid.UUID, uuid.UUID) error
}

type JobQueryRepository interface {
	Get(context.Context, uuid.UUID, uuid.UUID) (domain.Job, domain.JobCounts, error)
}

type TaskQueryRepository interface {
	Get(context.Context, uuid.UUID, uuid.UUID) (domain.Task, error)
	List(context.Context, uuid.UUID, Page) ([]domain.Task, error)
	Attempts(context.Context, uuid.UUID, uuid.UUID) ([]domain.TaskAttempt, error)
}

type WorkerRegistryStore interface {
	Register(context.Context, domain.WorkerInstance) error
	Heartbeat(ctx context.Context, instanceID uuid.UUID, running int, draining bool) error
}

type OutboxStore interface {
	Claim(context.Context, int, string, time.Duration) ([]domain.OutboxEvent, error)
}
type AuditStore interface {
	InsertIdempotently(context.Context, domain.AuditEvent) (bool, error)
}
