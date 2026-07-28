package executor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Result struct {
	Outcome               domain.TaskOutcome
	Result                json.RawMessage
	ResultHash            [32]byte
	ErrorType             domain.ErrorType
	ErrorMessage          string
	StartedAt, FinishedAt time.Time
}
type Executor interface {
	Execute(context.Context, scheduler.Assignment) Result
}
type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
}

func NewRegistry() *Registry { return &Registry{executors: map[string]Executor{}} }
func (r *Registry) Register(taskType string, executor Executor) error {
	if taskType == "" || executor == nil {
		return errors.New("task type and executor are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.executors[taskType]; exists {
		return errors.New("executor already registered")
	}
	r.executors[taskType] = executor
	return nil
}
func (r *Registry) Get(taskType string) (Executor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[taskType]
	return executor, ok
}
