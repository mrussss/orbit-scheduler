package agent

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type StepKind string
type StepStatus string

const (
	StepModel StepKind = "MODEL"
	StepTool  StepKind = "TOOL"
	StepFinal StepKind = "FINAL"
	StepError StepKind = "ERROR"

	StepRunning   StepStatus = "RUNNING"
	StepSucceeded StepStatus = "SUCCEEDED"
	StepFailed    StepStatus = "FAILED"
)

type TraceStep struct {
	TaskID        uuid.UUID
	AttemptNo     int
	StepNo        int
	Kind          StepKind
	ToolName      string
	InputSummary  json.RawMessage
	OutputSummary json.RawMessage
	Status        StepStatus
	StartedAt     time.Time
	FinishedAt    *time.Time
}

type Tracer interface {
	Record(context.Context, TraceStep) error
}

type TracerFunc func(context.Context, TraceStep) error

func (fn TracerFunc) Record(ctx context.Context, step TraceStep) error { return fn(ctx, step) }

type noopTracer struct{}

func (noopTracer) Record(context.Context, TraceStep) error { return nil }
