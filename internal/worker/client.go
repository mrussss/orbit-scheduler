package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Registration struct {
	WorkerName     string
	InstanceID     uuid.UUID
	Hostname       string
	Capacity       int
	TaskTypes      []string
	ProcessVersion string
	Metadata       []byte
}
type Heartbeat struct {
	InstanceID         uuid.UUID
	Running, Available int
	Draining           bool
	Uptime             time.Duration
}
type Client interface {
	Register(context.Context, Registration) error
	Heartbeat(context.Context, Heartbeat) error
	Fetch(context.Context, scheduler.FetchRequest) ([]scheduler.Assignment, error)
	Renew(context.Context, scheduler.RenewRequest) (scheduler.RenewResult, error)
	Report(context.Context, scheduler.ReportRequest) (scheduler.ReportResult, error)
	SetDraining(context.Context, uuid.UUID, bool) error
	Close() error
}
