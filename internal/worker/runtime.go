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
	GracePeriod                                                                                  time.Duration
	Observer                                                                                     Observer
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
	drainCh   chan struct{}
	fetchDone chan struct{}
	drainOnce sync.Once
	closeOnce sync.Once
	closeErr  error
	observer  Observer
}

func NewRuntime(client Client, executors *executor.Registry, logger *slog.Logger, cfg Config) (*Runtime, error) {
	if client == nil || executors == nil || logger == nil {
		return nil, errors.New("worker runtime dependencies are required")
	}
	if cfg.Registration.InstanceID == uuid.Nil || cfg.Registration.Capacity <= 0 || cfg.LeaseDuration <= 0 || cfg.RenewInterval <= 0 || cfg.RenewInterval >= cfg.LeaseDuration || cfg.FetchInterval <= 0 || cfg.HeartbeatInterval <= 0 || cfg.RPCDeadline <= 0 || cfg.ReportRetries < 0 || cfg.ReportRetryBase < 0 || cfg.GracePeriod <= 0 {
		return nil, errors.New("invalid worker runtime configuration")
	}
	observer := cfg.Observer
	if observer == nil {
		observer = noopObserver{}
	}
	runtime := &Runtime{client: client, executors: executors, logger: logger, cfg: cfg, sem: make(chan struct{}, cfg.Registration.Capacity), tasks: map[uuid.UUID]context.CancelFunc{}, drainCh: make(chan struct{}), fetchDone: make(chan struct{}), observer: observer}
	runtime.state.Store(int32(StateInitialized))
	return runtime, nil
}
func (r *Runtime) Start(ctx context.Context) error {
	if !r.state.CompareAndSwap(int32(StateInitialized), int32(StateRunning)) {
		return errors.New("worker runtime already started")
	}
	r.root, r.cancel = context.WithCancel(ctx)
	rpcCtx, cancel := context.WithTimeout(r.root, r.cfg.RPCDeadline)
	err := r.client.Register(rpcCtx, r.cfg.Registration)
	cancel()
	if err != nil {
		r.state.Store(int32(StateInitialized))
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
	defer close(r.fetchDone)
	ticker := time.NewTicker(r.cfg.FetchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.root.Done():
			return
		case <-r.drainCh:
			return
		case <-ticker.C:
			if r.draining.Load() {
				continue
			}
			available := cap(r.sem) - len(r.sem)
			if available <= 0 {
				continue
			}
			fetchStarted := time.Now()
			ctx, cancel := context.WithTimeout(r.root, r.cfg.RPCDeadline)
			assignments, err := r.client.Fetch(ctx, scheduler.FetchRequest{WorkerInstanceID: r.cfg.Registration.InstanceID, Requested: available, LeaseDuration: r.cfg.LeaseDuration})
			cancel()
			if err != nil {
				r.observer.Fetch("error", 0, time.Since(fetchStarted).Seconds())
				r.logger.Warn("fetch tasks failed", "error", err)
				continue
			}
			fetchOutcome := "success"
			if len(assignments) == 0 {
				fetchOutcome = "empty"
			}
			r.observer.Fetch(fetchOutcome, len(assignments), time.Since(fetchStarted).Seconds())
			for _, task := range assignments {
				// The database lease starts after the RPC begins. Using the local RPC
				// start plus the requested duration is deliberately conservative and
				// avoids comparing the worker clock with the database clock.
				task.LocalLeaseUntil = fetchStarted.Add(r.cfg.LeaseDuration)
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
	r.observer.TaskStarted(task.TaskType)
	r.wg.Add(1)
	go r.runTask(taskCtx, task)
}
func (r *Runtime) runTask(taskCtx context.Context, task scheduler.Assignment) {
	defer r.finishTask(task.TaskID, task.TaskType)
	executorStarted := time.Now()
	exec, ok := r.executors.Get(task.TaskType)
	if !ok {
		now := time.Now().UTC()
		result := executor.Result{Outcome: domain.OutcomePermanentFailure, ResultHash: domain.HashBytes(nil), ErrorType: domain.ErrorExecutor, ErrorMessage: "no executor registered for task type", StartedAt: now, FinishedAt: now}
		r.reportWithRetry(taskCtx, task, result)
		r.observer.ExecutorFinished(task.TaskType, string(result.Outcome), time.Since(executorStarted).Seconds())
		return
	}
	executionStartedAt := time.Now().UTC()
	deadline := executionStartedAt.Add(task.ExecutionTimeout)
	if task.OverallDeadline != nil && task.OverallDeadline.Before(deadline) {
		deadline = *task.OverallDeadline
	}
	executionCtx, cancelExecution := context.WithDeadline(taskCtx, deadline)
	defer cancelExecution()
	renewCtx, cancelRenew := context.WithCancel(taskCtx)
	lostLease := make(chan error, 1)
	cancelRequested := make(chan struct{}, 1)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		r.renewLoop(renewCtx, task, cancelExecution, lostLease, cancelRequested)
	}()
	resultCh := make(chan executor.Result, 1)
	go func() { resultCh <- exec.Execute(executionCtx, task) }()
	var result executor.Result
	select {
	case result = <-resultCh:
		result.StartedAt = executionStartedAt
		result.FinishedAt = time.Now().UTC()
	case <-executionCtx.Done():
		select {
		case <-cancelRequested:
			result = failureResult(executionStartedAt, domain.OutcomeCanceled, domain.ErrorCanceled, "task cancellation requested")
		default:
			if errors.Is(executionCtx.Err(), context.DeadlineExceeded) {
				result = failureResult(executionStartedAt, domain.OutcomeTimeout, domain.ErrorTimeout, "execution deadline exceeded")
			} else {
				result = failureResult(executionStartedAt, domain.OutcomeCanceled, domain.ErrorCanceled, "worker shutting down")
			}
		}
	}
	cancelRenew()
	<-renewDone
	r.observer.ExecutorFinished(task.TaskType, string(result.Outcome), time.Since(executorStarted).Seconds())
	select {
	case err := <-lostLease:
		r.logger.Warn("discarding result after lease loss", "task_id", task.TaskID, "attempt_no", task.AttemptNo, "error", err)
		return
	default:
	}
	if result.Outcome == domain.OutcomeCanceled && taskCtx.Err() != nil {
		result.Outcome = domain.OutcomeRetryableFailure
		result.ErrorType = domain.ErrorCanceled
		result.ErrorMessage = "worker shutdown interrupted execution"
	}
	r.reportWithRetry(taskCtx, task, result)
}

func (r *Runtime) renewLoop(ctx context.Context, task scheduler.Assignment, cancelExecution context.CancelFunc, lost chan<- error, cancelRequested chan<- struct{}) {
	ticker := time.NewTicker(r.cfg.RenewInterval)
	defer ticker.Stop()
	leaseValidUntil := task.LocalLeaseUntil
	if leaseValidUntil.IsZero() {
		leaseValidUntil = time.Now().Add(r.cfg.LeaseDuration)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewStarted := time.Now()
			rpcCtx, cancel := context.WithTimeout(ctx, r.cfg.RPCDeadline)
			result, err := r.client.Renew(rpcCtx, scheduler.RenewRequest{TaskID: task.TaskID, WorkerInstanceID: r.cfg.Registration.InstanceID, AttemptNo: task.AttemptNo, Extension: r.cfg.LeaseDuration})
			cancel()
			if err == nil {
				// RenewLease sets the database expiry to server-now plus the
				// requested extension. Anchoring it to the local RPC start expires
				// slightly early under latency, never late because of clock skew.
				leaseValidUntil = renewStarted.Add(r.cfg.LeaseDuration)
				if result.CancelRequested {
					select {
					case cancelRequested <- struct{}{}:
					default:
					}
					cancelExecution()
					return
				}
				continue
			}
			r.observer.RenewError()
			if errors.Is(err, scheduler.ErrStaleLease) || errors.Is(err, scheduler.ErrAlreadyFinalized) || !time.Now().Before(leaseValidUntil) {
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
	if result.StartedAt.IsZero() {
		result.StartedAt = time.Now().UTC()
	}
	if result.FinishedAt.IsZero() {
		result.FinishedAt = time.Now().UTC()
	}
	request := scheduler.ReportRequest{TaskID: task.TaskID, WorkerInstanceID: r.cfg.Registration.InstanceID, AttemptNo: task.AttemptNo, Outcome: result.Outcome, Result: result.Result, ResultHash: result.ResultHash, ErrorType: result.ErrorType, ErrorMessage: result.ErrorMessage, ExecutionStartedAt: result.StartedAt, ExecutionFinishedAt: result.FinishedAt}
	baseCtx := ctx
	if ctx.Err() != nil {
		baseCtx = context.Background()
	}
	for attempt := 0; attempt <= r.cfg.ReportRetries; attempt++ {
		rpcCtx, cancel := context.WithTimeout(baseCtx, r.cfg.RPCDeadline)
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
		r.observer.ReportRetry()
		delay := r.cfg.ReportRetryBase
		for n := 0; n < attempt && delay < r.cfg.RPCDeadline/2; n++ {
			delay *= 2
		}
		select {
		case <-baseCtx.Done():
			return
		case <-time.After(delay):
		}
	}
}
func failureResult(started time.Time, outcome domain.TaskOutcome, errorType domain.ErrorType, message string) executor.Result {
	return executor.Result{Outcome: outcome, ResultHash: domain.HashBytes(nil), ErrorType: errorType, ErrorMessage: message, StartedAt: started, FinishedAt: time.Now().UTC()}
}
func (r *Runtime) finishTask(taskID uuid.UUID, taskType string) {
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
	r.observer.TaskFinished(taskType)
	r.wg.Done()
}

func (r *Runtime) SetDraining(ctx context.Context, draining bool) error {
	r.draining.Store(draining)
	if draining && r.State() < StateDraining {
		r.state.Store(int32(StateDraining))
	}
	if draining {
		r.drainOnce.Do(func() { close(r.drainCh) })
	}
	rpcCtx, cancel := context.WithTimeout(ctx, r.cfg.RPCDeadline)
	defer cancel()
	return r.client.SetDraining(rpcCtx, r.cfg.Registration.InstanceID, draining)
}
func (r *Runtime) Running() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.tasks) }
func (r *Runtime) StopNow(ctx context.Context) error {
	if r.State() == StateStopped {
		return r.closeClient()
	}
	if r.State() == StateInitialized {
		r.state.Store(int32(StateStopped))
		return r.closeClient()
	}
	r.draining.Store(true)
	r.drainOnce.Do(func() { close(r.drainCh) })
	if r.cancel != nil {
		r.cancel()
	}
	<-r.fetchDone
	done := make(chan struct{})
	go func() { r.loops.Wait(); r.wg.Wait(); close(done) }()
	select {
	case <-done:
		r.state.Store(int32(StateStopped))
		return r.closeClient()
	case <-ctx.Done():
		r.state.Store(int32(StateStopping))
		return errors.Join(ctx.Err(), r.closeClient())
	}
}

func (r *Runtime) closeClient() error {
	r.closeOnce.Do(func() { r.closeErr = r.client.Close() })
	return r.closeErr
}
