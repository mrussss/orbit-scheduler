package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/executor"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestLifecycleStateValuesAreOrdered(t *testing.T) {
	if !(StateInitialized < StateRunning && StateRunning < StateDraining && StateDraining < StateStopping && StateStopping < StateStopped) {
		t.Fatal("lifecycle states must progress monotonically")
	}
}

func TestStopNowBeforeStart(t *testing.T) {
	client := &fakeClient{reports: make(chan scheduler.ReportRequest, 1)}
	runtime, err := NewRuntime(client, executor.NewRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Registration:  Registration{WorkerName: "test", InstanceID: uuid.New(), Capacity: 1, TaskTypes: []string{"mock"}},
		LeaseDuration: time.Second, RenewInterval: 100 * time.Millisecond, FetchInterval: time.Millisecond,
		HeartbeatInterval: time.Second, RPCDeadline: time.Second, ReportRetryBase: time.Millisecond, GracePeriod: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.StopNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.State() != StateStopped {
		t.Fatalf("state=%v", runtime.State())
	}
}

type ignoringExecutor struct{}

func (ignoringExecutor) Execute(context.Context, scheduler.Assignment) executor.Result {
	time.Sleep(2 * time.Second)
	return executor.Result{Outcome: domain.OutcomeSucceeded, ResultHash: domain.HashBytes(nil), StartedAt: time.Now(), FinishedAt: time.Now()}
}
func TestGracefulShutdownDoesNotWaitForIgnoringExecutor(t *testing.T) {
	instanceID := uuid.New()
	client := &fakeClient{tasks: []scheduler.Assignment{{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "ignore", AttemptNo: 1, LeaseExpiresAt: time.Now().Add(time.Second), ExecutionTimeout: time.Hour}}, reports: make(chan scheduler.ReportRequest, 1)}
	registry := executor.NewRegistry()
	_ = registry.Register("ignore", ignoringExecutor{})
	runtime, err := NewRuntime(client, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Registration: Registration{WorkerName: "test", InstanceID: instanceID, Capacity: 1, TaskTypes: []string{"ignore"}}, LeaseDuration: time.Second, RenewInterval: 100 * time.Millisecond, FetchInterval: time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, RPCDeadline: 20 * time.Millisecond, ReportRetries: 0, ReportRetryBase: time.Millisecond, GracePeriod: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(root); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for runtime.Running() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	graceCtx, graceCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer graceCancel()
	started := time.Now()
	if err := runtime.GracefulShutdown(graceCtx); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("shutdown waited for context-ignoring executor")
	}
	if runtime.State() != StateStopped {
		t.Fatalf("state=%v", runtime.State())
	}
}
