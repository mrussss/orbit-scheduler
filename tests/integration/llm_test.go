//go:build integration

package integration_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	baseexecutor "github.com/mrussss/orbit-scheduler/internal/executor"
	llmexecutor "github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestLLMRetryAttemptAndStaleResultFencing(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, migratedPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.New(pool, pgstore.Config{MaxFetchBatch: 10, RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	projectID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES($1,'llm-project','ACTIVE',100,2,now(),now())`, projectID); err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "llm", Payload: []byte(`{"model":"model-a","messages":[{"role":"user","content":"retry"}],"max_output_tokens":20}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 3, IdempotencyKey: "llm-retry"})
	if err != nil {
		t.Fatal(err)
	}
	workerID := uuid.New()
	if err := store.RegisterWorker(ctx, domain.WorkerInstance{ID: workerID, WorkerName: "llm-worker", Hostname: "test", Capacity: 1, SupportedTaskTypes: []string{"llm"}, ProcessVersion: "test"}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	providerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		if calls.Add(1) == 1 {
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"model":"model-a","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	}))
	defer providerServer.Close()
	provider, err := llmexecutor.NewOpenAICompatible(llmexecutor.OpenAICompatibleConfig{BaseURL: providerServer.URL, APIKey: "test-secret", RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxResponseBytes: 1 << 20, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.CloseIdleConnections()
	executor, err := llmexecutor.NewExecutor(llmexecutor.ExecutorConfig{ProviderName: "openai-compatible", Provider: provider, AllowedModels: []string{"model-a"}, MaxPromptBytes: 1024, MaxOutputTokens: 100, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	first := fetchOne(t, ctx, store, workerID)
	firstResult := executor.Execute(ctx, first)
	if firstResult.Outcome != domain.OutcomeRetryableFailure {
		t.Fatalf("first outcome=%s", firstResult.Outcome)
	}
	firstReport, err := store.ReportResult(ctx, reportRequest(first, workerID, firstResult))
	if err != nil || firstReport.Status != domain.TaskPending || firstReport.AvailableAt == nil {
		t.Fatalf("first report=%+v err=%v", firstReport, err)
	}

	second := fetchOne(t, ctx, store, workerID)
	if second.AttemptNo != 2 {
		t.Fatalf("attempt=%d, want 2", second.AttemptNo)
	}
	secondResult := executor.Execute(ctx, second)
	if secondResult.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("second result=%+v", secondResult)
	}
	secondReport, err := store.ReportResult(ctx, reportRequest(second, workerID, secondResult))
	if err != nil || secondReport.Status != domain.TaskSucceeded {
		t.Fatalf("second report=%+v err=%v", secondReport, err)
	}

	stale := firstResult
	stale.Outcome = domain.OutcomeSucceeded
	stale.Result = []byte(`{"stale":true}`)
	stale.ResultHash = domain.HashBytes(stale.Result)
	_, err = store.ReportResult(ctx, reportRequest(first, workerID, stale))
	if !errors.Is(err, scheduler.ErrConflict) && !errors.Is(err, scheduler.ErrStaleLease) && !errors.Is(err, scheduler.ErrAlreadyFinalized) {
		t.Fatalf("stale LLM result was not rejected: %v", err)
	}
	var status string
	var attempts int
	var result []byte
	if err := pool.QueryRow(ctx, `SELECT status,attempt_no,result FROM tasks WHERE id=$1`, created.Task.ID).Scan(&status, &attempts, &result); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" || attempts != 2 || len(result) == 0 {
		t.Fatalf("status=%s attempts=%d result=%s", status, attempts, result)
	}
	var secretPersisted bool
	if err := pool.QueryRow(ctx, `
		SELECT
			t.payload::text LIKE '%' || $2 || '%'
			OR COALESCE(t.result::text, '') LIKE '%' || $2 || '%'
			OR COALESCE(t.final_error_message, '') LIKE '%' || $2 || '%'
			OR EXISTS (
				SELECT 1 FROM task_attempts a
				WHERE a.task_id=t.id AND COALESCE(a.error_message, '') LIKE '%' || $2 || '%'
			)
		FROM tasks t WHERE t.id=$1`, created.Task.ID, "test-secret").Scan(&secretPersisted); err != nil {
		t.Fatal(err)
	}
	if secretPersisted {
		t.Fatal("provider API key was persisted in task or attempt state")
	}
}

func fetchOne(t *testing.T, ctx context.Context, store *pgstore.Store, workerID uuid.UUID) scheduler.Assignment {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		assignments, err := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: workerID, Requested: 1, LeaseDuration: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if len(assignments) == 1 {
			return assignments[0]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("LLM task was not fetchable before deadline")
	return scheduler.Assignment{}
}

func reportRequest(assignment scheduler.Assignment, workerID uuid.UUID, result baseexecutor.Result) scheduler.ReportRequest {
	return scheduler.ReportRequest{
		TaskID: assignment.TaskID, WorkerInstanceID: workerID, AttemptNo: assignment.AttemptNo,
		Outcome: result.Outcome, Result: result.Result, ResultHash: result.ResultHash,
		ErrorType: result.ErrorType, ErrorMessage: result.ErrorMessage,
		ExecutionStartedAt: result.StartedAt, ExecutionFinishedAt: result.FinishedAt,
	}
}
