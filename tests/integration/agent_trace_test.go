//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	baseexecutor "github.com/mrussss/orbit-scheduler/internal/executor"
	agentexecutor "github.com/mrussss/orbit-scheduler/internal/executor/agent"
	"github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestAgentTraceMonotonicUpdatesAndStaleAttemptFencing(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, migratedPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.New(pool, pgstore.Config{MaxFetchBatch: 10, RetryBase: time.Millisecond, RetryMax: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	projectID, workerID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES($1,'agent','ACTIVE',10,1,now(),now())`, projectID); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterWorker(ctx, domain.WorkerInstance{ID: workerID, WorkerName: "agent-worker", Hostname: "test", Capacity: 1, SupportedTaskTypes: []string{"agent"}, ProcessVersion: "test"}); err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "agent", Payload: []byte(`{"repository_root":"gateway","issue":"stall"}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	first := fetchOne(t, ctx, store, workerID)
	started := time.Now().UTC()
	running := scheduler.RecordAgentStepRequest{TaskID: first.TaskID, WorkerInstanceID: workerID, AttemptNo: first.AttemptNo, StepNo: 1, Kind: scheduler.AgentStepModel, InputSummary: []byte(`{"round":1}`), Status: scheduler.AgentStepRunning, StartedAt: started}
	if err := store.RecordAgentStep(ctx, running); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAgentStep(ctx, scheduler.RecordAgentStepRequest{TaskID: first.TaskID, WorkerInstanceID: workerID, AttemptNo: first.AttemptNo, StepNo: 3, Kind: scheduler.AgentStepError, OutputSummary: []byte(`{"reason":"gap"}`), Status: scheduler.AgentStepFailed, StartedAt: started, FinishedAt: ptrTime(started)}); !errors.Is(err, scheduler.ErrConflict) {
		t.Fatalf("non-monotonic trace error=%v", err)
	}
	finished := started.Add(time.Millisecond)
	running.Status, running.OutputSummary, running.FinishedAt = scheduler.AgentStepSucceeded, []byte(`{"tool_calls":1}`), &finished
	if err := store.RecordAgentStep(ctx, running); err != nil {
		t.Fatal(err)
	}
	toolStarted := finished
	toolFinished := toolStarted.Add(time.Millisecond)
	tool := scheduler.RecordAgentStepRequest{TaskID: first.TaskID, WorkerInstanceID: workerID, AttemptNo: first.AttemptNo, StepNo: 2, Kind: scheduler.AgentStepTool, ToolName: "read_file", InputSummary: []byte(`{"arguments_bytes":20}`), Status: scheduler.AgentStepRunning, StartedAt: toolStarted}
	if err := store.RecordAgentStep(ctx, tool); err != nil {
		t.Fatal(err)
	}
	tool.Status, tool.OutputSummary, tool.FinishedAt = scheduler.AgentStepSucceeded, []byte(`{"result_bytes":100}`), &toolFinished
	if err := store.RecordAgentStep(ctx, tool); err != nil {
		t.Fatal(err)
	}
	// Simulate a worker crash after the read-only tool/model step: no result is
	// reported, the lease expires, and the reaper creates room for attempt 2.
	if _, err := pool.Exec(ctx, `UPDATE tasks SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, first.TaskID); err != nil {
		t.Fatal(err)
	}
	if reaped, err := store.ReapExpired(ctx, 1); err != nil || reaped.Requeued != 1 {
		t.Fatalf("reaped=%+v err=%v", reaped, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET available_at=now() WHERE id=$1`, first.TaskID); err != nil {
		t.Fatal(err)
	}
	stale := scheduler.RecordAgentStepRequest{TaskID: first.TaskID, WorkerInstanceID: workerID, AttemptNo: first.AttemptNo, StepNo: 3, Kind: scheduler.AgentStepError, OutputSummary: []byte(`{"reason":"stale"}`), Status: scheduler.AgentStepFailed, StartedAt: toolFinished, FinishedAt: ptrTime(toolFinished)}
	if err := store.RecordAgentStep(ctx, stale); !errors.Is(err, scheduler.ErrStaleLease) {
		t.Fatalf("stale trace error=%v", err)
	}
	second := fetchOne(t, ctx, store, workerID)
	if second.AttemptNo != 2 {
		t.Fatalf("attempt=%d", second.AttemptNo)
	}
	secondStep := running
	secondStep.AttemptNo, secondStep.Status, secondStep.FinishedAt = 2, scheduler.AgentStepRunning, nil
	if err := store.RecordAgentStep(ctx, secondStep); err != nil {
		t.Fatal(err)
	}
	var firstCount, secondCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_steps WHERE task_id=$1 AND attempt_no=1`, created.Task.ID).Scan(&firstCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_steps WHERE task_id=$1 AND attempt_no=2`, created.Task.ID).Scan(&secondCount); err != nil {
		t.Fatal(err)
	}
	if firstCount != 2 || secondCount != 1 {
		t.Fatalf("first=%d second=%d", firstCount, secondCount)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

type retryAgentProvider struct{ calls atomic.Int32 }

func (p *retryAgentProvider) GenerateWithTools(_ context.Context, request llm.ToolRequest) (llm.ToolResponse, error) {
	if p.calls.Add(1) == 1 {
		return llm.ToolResponse{}, &llm.ProviderError{Kind: llm.ErrorRateLimited, Retryable: true}
	}
	return llm.ToolResponse{Model: request.Model, FinishReason: "stop", Content: `{"problem_type":"queue","likely_causes":["bounded queue"],"code_evidence":[],"recommended_checks":["inspect metrics"],"confidence":0.6,"limits":[]}`, Usage: llm.Usage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10}}, nil
}

func TestAgent429RetriesAsNewFencedAttempt(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, migratedPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.New(pool, pgstore.Config{MaxFetchBatch: 10, RetryBase: time.Millisecond, RetryMax: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	projectID, workerID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES($1,'agent-retry','ACTIVE',10,1,now(),now())`, projectID); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterWorker(ctx, domain.WorkerInstance{ID: workerID, WorkerName: "agent-retry", Hostname: "test", Capacity: 1, SupportedTaskTypes: []string{"agent"}, ProcessVersion: "test"}); err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "agent", Payload: []byte(`{"repository_root":"gateway","issue":"queue"}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	repository := t.TempDir()
	if err := os.WriteFile(filepath.Join(repository, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	toolbox, err := agentexecutor.NewToolbox(map[string]string{"gateway": repository}, agentexecutor.ToolLimits{MaxFileBytes: 1024, MaxResultBytes: 4096, MaxMatches: 10})
	if err != nil {
		t.Fatal(err)
	}
	tracer := agentexecutor.TracerFunc(func(ctx context.Context, step agentexecutor.TraceStep) error {
		return store.RecordAgentStep(ctx, scheduler.RecordAgentStepRequest{TaskID: step.TaskID, WorkerInstanceID: workerID, AttemptNo: step.AttemptNo, StepNo: step.StepNo, Kind: scheduler.AgentStepKind(step.Kind), ToolName: step.ToolName, InputSummary: step.InputSummary, OutputSummary: step.OutputSummary, Status: scheduler.AgentStepStatus(step.Status), StartedAt: step.StartedAt, FinishedAt: step.FinishedAt})
	})
	executor, err := agentexecutor.NewExecutor(agentexecutor.ExecutorConfig{Provider: &retryAgentProvider{}, Toolbox: toolbox, Tracer: tracer, Repositories: map[string]string{"gateway": repository}, Model: "fake", MaxIssueBytes: 1024, MaxOutputTokens: 100, MaxModelSteps: 3, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := fetchOne(t, ctx, store, workerID)
	firstResult := executor.Execute(ctx, first)
	if firstResult.Outcome != domain.OutcomeRetryableFailure {
		t.Fatalf("first=%+v", firstResult)
	}
	if _, err := store.ReportResult(ctx, agentReport(first, workerID, firstResult)); err != nil {
		t.Fatal(err)
	}
	second := fetchOne(t, ctx, store, workerID)
	secondResult := executor.Execute(ctx, second)
	if second.AttemptNo != 2 || secondResult.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("second attempt=%d result=%+v", second.AttemptNo, secondResult)
	}
	if _, err := store.ReportResult(ctx, agentReport(second, workerID, secondResult)); err != nil {
		t.Fatal(err)
	}
	var result json.RawMessage
	var attempt int
	if err := pool.QueryRow(ctx, `SELECT result,attempt_no FROM tasks WHERE id=$1`, created.Task.ID).Scan(&result, &attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 || len(result) == 0 {
		t.Fatalf("attempt=%d result=%s", attempt, result)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(DISTINCT attempt_no) FROM agent_steps WHERE task_id=$1`, created.Task.ID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("trace attempts=%d err=%v", attempts, err)
	}
}

func agentReport(assignment scheduler.Assignment, workerID uuid.UUID, result baseexecutor.Result) scheduler.ReportRequest {
	return scheduler.ReportRequest{TaskID: assignment.TaskID, WorkerInstanceID: workerID, AttemptNo: assignment.AttemptNo, Outcome: result.Outcome, Result: result.Result, ResultHash: result.ResultHash, ErrorType: result.ErrorType, ErrorMessage: result.ErrorMessage, ExecutionStartedAt: result.StartedAt, ExecutionFinishedAt: result.FinishedAt}
}
