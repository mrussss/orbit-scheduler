package pgstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestNewValidatesConfig(t *testing.T) {
	_, err := New(nil, Config{MaxFetchBatch: 10, RetryBase: time.Second, RetryMax: time.Minute})
	if err == nil {
		t.Fatal("expected missing pool error")
	}
}

func TestResultDecision(t *testing.T) {
	store := &Store{cfg: Config{RetryBase: time.Second, RetryMax: time.Second}}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	request := scheduler.ReportRequest{TaskID: uuid.New(), WorkerInstanceID: uuid.New(), AttemptNo: 1, Outcome: domain.OutcomeRetryableFailure, Result: json.RawMessage(`{}`)}
	status, event, available, err := store.resultDecision(taskForReport{attemptNo: 1, maxAttempts: 3}, request, now)
	if err != nil || status != domain.TaskPending || event != "TASK_RETRY_SCHEDULED" || available == nil || !available.Equal(now.Add(time.Second)) {
		t.Fatalf("retry decision = %s %s %v %v", status, event, available, err)
	}
	status, _, _, err = store.resultDecision(taskForReport{attemptNo: 3, maxAttempts: 3}, request, now)
	if err != nil || status != domain.TaskFailed {
		t.Fatalf("exhausted status = %s, err=%v", status, err)
	}
	request.Outcome = domain.OutcomeCanceled
	if _, _, _, err := store.resultDecision(taskForReport{attemptNo: 1, maxAttempts: 3}, request, now); err != scheduler.ErrInvalidOutcome {
		t.Fatalf("unsolicited cancel err = %v", err)
	}
}

func TestNormalizeTaskHashUsesSemanticJSON(t *testing.T) {
	now := time.Now().UTC()
	a := command.CreateTask{TaskType: "mock", Payload: json.RawMessage(`{"a":1,"b":2}`), AvailableAt: now, ExecutionTimeout: time.Second, MaxAttempts: 2}
	b := a
	b.Payload = json.RawMessage(`{ "b": 2, "a": 1 }`)
	_, aHash, _, err := normalizeTask(a)
	if err != nil {
		t.Fatal(err)
	}
	_, bHash, _, err := normalizeTask(b)
	if err != nil {
		t.Fatal(err)
	}
	if aHash != bHash {
		t.Fatal("equivalent requests produced different creation hashes")
	}
}
