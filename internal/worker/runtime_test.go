package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/executor"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type fakeClient struct {
	mu       sync.Mutex
	tasks    []scheduler.Assignment
	reports  chan scheduler.ReportRequest
	renewErr error
	closed   int
}

func (f *fakeClient) Register(context.Context, Registration) error { return nil }
func (f *fakeClient) Heartbeat(context.Context, Heartbeat) error   { return nil }
func (f *fakeClient) Fetch(_ context.Context, request scheduler.FetchRequest) ([]scheduler.Assignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := min(request.Requested, len(f.tasks))
	out := append([]scheduler.Assignment(nil), f.tasks[:count]...)
	f.tasks = f.tasks[count:]
	return out, nil
}
func (f *fakeClient) Renew(context.Context, scheduler.RenewRequest) (scheduler.RenewResult, error) {
	return scheduler.RenewResult{LeaseExpiresAt: time.Now().Add(time.Second)}, f.renewErr
}
func (f *fakeClient) Report(_ context.Context, request scheduler.ReportRequest) (scheduler.ReportResult, error) {
	f.reports <- request
	return scheduler.ReportResult{Status: domain.TaskSucceeded}, nil
}
func (f *fakeClient) SetDraining(context.Context, uuid.UUID, bool) error { return nil }
func (f *fakeClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}
func (f *fakeClient) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type blockingExecutor struct {
	current, max atomic.Int32
	release      <-chan struct{}
}

func (e *blockingExecutor) Execute(ctx context.Context, _ scheduler.Assignment) executor.Result {
	started := time.Now()
	current := e.current.Add(1)
	for {
		maximum := e.max.Load()
		if current <= maximum || e.max.CompareAndSwap(maximum, current) {
			break
		}
	}
	select {
	case <-ctx.Done():
	case <-e.release:
	}
	e.current.Add(-1)
	result := json.RawMessage(`{"ok":true}`)
	return executor.Result{Outcome: domain.OutcomeSucceeded, Result: result, ResultHash: domain.HashBytes(result), StartedAt: started, FinishedAt: time.Now()}
}

func TestRuntimeBoundsConcurrencyAndReports(t *testing.T) {
	instanceID := uuid.New()
	tasks := make([]scheduler.Assignment, 8)
	for i := range tasks {
		tasks[i] = scheduler.Assignment{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "mock", AttemptNo: 1, LeaseExpiresAt: time.Now().Add(time.Second), ExecutionTimeout: time.Second}
	}
	client := &fakeClient{tasks: tasks, reports: make(chan scheduler.ReportRequest, 8)}
	release := make(chan struct{})
	exec := &blockingExecutor{release: release}
	registry := executor.NewRegistry()
	if err := registry.Register("mock", exec); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(client, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Registration: Registration{WorkerName: "test", InstanceID: instanceID, Capacity: 4, TaskTypes: []string{"mock"}}, LeaseDuration: time.Second, RenewInterval: 100 * time.Millisecond, FetchInterval: 5 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, RPCDeadline: 50 * time.Millisecond, ReportRetries: 1, ReportRetryBase: time.Millisecond, GracePeriod: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for exec.max.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := exec.max.Load(); got != 4 {
		t.Fatalf("max concurrency=%d", got)
	}
	close(release)
	for len(client.reports) < 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(client.reports) != 8 {
		t.Fatalf("reports=%d", len(client.reports))
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := runtime.StopNow(stopCtx); err != nil {
		t.Fatal(err)
	}
	if runtime.Running() != 0 {
		t.Fatalf("running=%d", runtime.Running())
	}
	if client.closeCount() != 1 {
		t.Fatalf("client close count=%d", client.closeCount())
	}
}

func TestRuntimeEnforcesExecutionTimeout(t *testing.T) {
	instanceID := uuid.New()
	task := scheduler.Assignment{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "mock", Payload: []byte(`{"mode":"ignore_context","delay_ms":500}`), AttemptNo: 1, LeaseExpiresAt: time.Now().Add(time.Second), ExecutionTimeout: 10 * time.Millisecond}
	client := &fakeClient{tasks: []scheduler.Assignment{task}, reports: make(chan scheduler.ReportRequest, 1)}
	registry := executor.NewRegistry()
	_ = registry.Register("mock", &executor.Mock{})
	runtime, err := NewRuntime(client, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Registration: Registration{WorkerName: "test", InstanceID: instanceID, Capacity: 1, TaskTypes: []string{"mock"}}, LeaseDuration: time.Second, RenewInterval: 100 * time.Millisecond, FetchInterval: time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, RPCDeadline: 20 * time.Millisecond, ReportRetries: 0, ReportRetryBase: time.Millisecond, GracePeriod: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-client.reports:
		if report.Outcome != domain.OutcomeTimeout {
			t.Fatalf("outcome=%s", report.Outcome)
		}
		if elapsed := report.ExecutionFinishedAt.Sub(report.ExecutionStartedAt); elapsed < 8*time.Millisecond {
			t.Fatalf("timeout duration=%s, want actual execution time", elapsed)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("execution timeout did not report")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := runtime.StopNow(stopCtx); err != nil {
		t.Fatal(err)
	}
}
