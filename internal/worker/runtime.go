package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/executor"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Config struct {
	Registration                                                                                 Registration
	LeaseDuration, RenewInterval, FetchInterval, HeartbeatInterval, RPCDeadline, ReportRetryBase time.Duration
	ReportRetries                                                                                int
}
type Runtime struct {
	client    Client
	executors *executor.Registry
	logger    *slog.Logger
	cfg       Config
	root      context.Context
	cancel    context.CancelFunc
	draining  atomic.Bool
	state     atomic.Int32
	started   time.Time
	sem       chan struct{}
	wg        sync.WaitGroup
	loops     sync.WaitGroup
	mu        sync.Mutex
	tasks     map[uuid.UUID]context.CancelFunc
}

func NewRuntime(client Client, executors *executor.Registry, logger *slog.Logger, cfg Config) (*Runtime, error) {
	if client == nil || executors == nil || logger == nil {
		return nil, errors.New("worker runtime dependencies are required")
	}
	if cfg.Registration.InstanceID == uuid.Nil || cfg.Registration.Capacity <= 0 || cfg.LeaseDuration <= 0 || cfg.RenewInterval <= 0 || cfg.RenewInterval >= cfg.LeaseDuration || cfg.FetchInterval <= 0 || cfg.HeartbeatInterval <= 0 || cfg.RPCDeadline <= 0 || cfg.ReportRetries < 0 {
		return nil, errors.New("invalid worker runtime configuration")
	}
	runtime := &Runtime{client: client, executors: executors, logger: logger, cfg: cfg, sem: make(chan struct{}, cfg.Registration.Capacity), tasks: map[uuid.UUID]context.CancelFunc{}}
	runtime.state.Store(int32(StateInitialized))
	return runtime, nil
}
func (r *Runtime) Start(ctx context.Context) error {
	r.root, r.cancel = context.WithCancel(ctx)
	rpcCtx, cancel := context.WithTimeout(r.root, r.cfg.RPCDeadline)
	err := r.client.Register(rpcCtx, r.cfg.Registration)
	cancel()
	if err != nil {
		return err
	}
	r.started = time.Now()
	r.loops.Add(2)
	go r.fetchLoop()
	go r.heartbeatLoop()
	return nil
}
func (r *Runtime) fetchLoop() {
	defer r.loops.Done()
	ticker := time.NewTicker(r.cfg.FetchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.root.Done():
			return
		case <-ticker.C:
			if r.draining.Load() {
				continue
			}
			available := cap(r.sem) - len(r.sem)
			if available <= 0 {
				continue
			}
			ctx, cancel := context.WithTimeout(r.root, r.cfg.RPCDeadline)
			assignments, err := r.client.Fetch(ctx, scheduler.FetchRequest{WorkerInstanceID: r.cfg.Registration.InstanceID, Requested: available, LeaseDuration: r.cfg.LeaseDuration})
			cancel()
			if err != nil {
				r.logger.Warn("fetch tasks failed", "error", err)
				continue
			}
			for _, task := range assignments {
				r.startTask(task)
			}
		}
	}
}
func (r *Runtime) heartbeatLoop() {
	defer r.loops.Done()
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.root.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			running := len(r.tasks)
			r.mu.Unlock()
			ctx, cancel := context.WithTimeout(r.root, r.cfg.RPCDeadline)
			err := r.client.Heartbeat(ctx, Heartbeat{InstanceID: r.cfg.Registration.InstanceID, Running: running, Available: r.cfg.Registration.Capacity - running, Draining: r.draining.Load(), Uptime: time.Since(r.started)})
			cancel()
			if err != nil {
				r.logger.Warn("heartbeat failed", "error", err)
			}
		}
	}
}
func (r *Runtime) startTask(task scheduler.Assignment) {
	r.mu.Lock()
	if _, exists := r.tasks[task.TaskID]; exists {
		r.mu.Unlock()
		return
	}
	taskCtx, cancel := context.WithCancel(r.root)
	r.tasks[task.TaskID] = cancel
	r.mu.Unlock()
	r.sem <- struct{}{}
	r.wg.Add(1)
	go r.runTask(taskCtx, task)
}
func (r *Runtime) runTask(taskCtx context.Context, task scheduler.Assignment) {
	defer r.finishTask(task.TaskID)
	exec, ok := r.executors.Get(task.TaskType)
	if !ok {
		now := time.Now().UTC()
		result := executor.Result{Outcome: domain.OutcomePermanentFailure, ResultHash: domain.HashBytes(nil), ErrorType: domain.ErrorExecutor, ErrorMessage: "no executor registered for task type", StartedAt: now, FinishedAt: now}
		r.reportWithRetry(taskCtx, task, result)
		return
	}
	executionCtx, cancelExecution := context.WithCancel(taskCtx)
	defer cancelExecution()
	renewCtx, cancelRenew := context.WithCancel(taskCtx)
	lostLease := make(chan error, 1)
	renewDone := make(chan struct{})
	go func() { defer close(renewDone); r.renewLoop(renewCtx, task, cancelExecution, lostLease) }()
	result := exec.Execute(executionCtx, task)
	cancelRenew()
	<-renewDone
	select {
	case err := <-lostLease:
		r.logger.Warn("discarding result after lease loss", "task_id", task.TaskID, "attempt_no", task.AttemptNo, "error", err)
		return
	default:
	}
	r.reportWithRetry(taskCtx, task, result)
}

func (r *Runtime) renewLoop(ctx context.Context, task scheduler.Assignment, cancelExecution context.CancelFunc, lost chan<- error) {
	ticker := time.NewTicker(r.cfg.RenewInterval)
	defer ticker.Stop()
	expiresAt := task.LeaseExpiresAt
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rpcCtx, cancel := context.WithTimeout(ctx, r.cfg.RPCDeadline)
			result, err := r.client.Renew(rpcCtx, scheduler.RenewRequest{TaskID: task.TaskID, WorkerInstanceID: r.cfg.Registration.InstanceID, AttemptNo: task.AttemptNo, Extension: r.cfg.LeaseDuration})
			cancel()
			if err == nil {
				expiresAt = result.LeaseExpiresAt
				if result.CancelRequested {
					cancelExecution()
					return
				}
				continue
			}
			if errors.Is(err, scheduler.ErrStaleLease) || errors.Is(err, scheduler.ErrAlreadyFinalized) || !time.Now().Before(expiresAt) {
				select {
				case lost <- err:
				default:
				}
				cancelExecution()
				return
			}
			r.logger.Warn("lease renewal uncertain", "task_id", task.TaskID, "attempt_no", task.AttemptNo, "error", err)
		}
	}
}

func (r *Runtime) reportWithRetry(ctx context.Context, task scheduler.Assignment, result executor.Result) {
	request := scheduler.ReportRequest{TaskID: task.TaskID, WorkerInstanceID: r.cfg.Registration.InstanceID, AttemptNo: task.AttemptNo, Outcome: result.Outcome, Result: result.Result, ResultHash: result.ResultHash, ErrorType: result.ErrorType, ErrorMessage: result.ErrorMessage, ExecutionStartedAt: result.StartedAt, ExecutionFinishedAt: result.FinishedAt}
	for attempt := 0; attempt <= r.cfg.ReportRetries; attempt++ {
		rpcCtx, cancel := context.WithTimeout(ctx, r.cfg.RPCDeadline)
		_, err := r.client.Report(rpcCtx, request)
		cancel()
		if err == nil {
			return
		}
		if errors.Is(err, scheduler.ErrStaleLease) || errors.Is(err, scheduler.ErrConflict) || errors.Is(err, scheduler.ErrAlreadyFinalized) {
			r.logger.Warn("result rejected", "task_id", task.TaskID, "attempt_no", task.AttemptNo, "error", err)
			return
		}
		if attempt == r.cfg.ReportRetries {
			r.logger.Error("result report retries exhausted", "task_id", task.TaskID, "attempt_no", task.AttemptNo, "error", err)
			return
		}
		delay := r.cfg.ReportRetryBase
		for n := 0; n < attempt && delay < r.cfg.RPCDeadline/2; n++ {
			delay *= 2
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
func (r *Runtime) finishTask(taskID uuid.UUID) {
	r.mu.Lock()
	if cancel, ok := r.tasks[taskID]; ok {
		cancel()
		delete(r.tasks, taskID)
	}
	r.mu.Unlock()
	select {
	case <-r.sem:
	default:
	}
	r.wg.Done()
}

func (r *Runtime) SetDraining(ctx context.Context, draining bool) error {
	r.draining.Store(draining)
	rpcCtx, cancel := context.WithTimeout(ctx, r.cfg.RPCDeadline)
	defer cancel()
	return r.client.SetDraining(rpcCtx, r.cfg.Registration.InstanceID, draining)
}
func (r *Runtime) Running() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.tasks) }
func (r *Runtime) StopNow(ctx context.Context) error {
	r.draining.Store(true)
	if r.cancel != nil {
		r.cancel()
	}
	done := make(chan struct{})
	go func() { r.loops.Wait(); r.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
