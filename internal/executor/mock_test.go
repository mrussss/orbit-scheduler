package executor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func mockTask(payload string, attempt int) scheduler.Assignment {
	return scheduler.Assignment{TaskID: uuid.New(), TaskType: "mock", Payload: []byte(payload), AttemptNo: attempt}
}
func TestMockModes(t *testing.T) {
	crashed := false
	mock := &Mock{CrashHook: func(scheduler.Assignment) { crashed = true }}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"success","result":{"x":1}}`, 1)); got.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("success=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"delay_success","delay_ms":1}`, 1)); got.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("delay success=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"permanent_failure"}`, 1)); got.Outcome != domain.OutcomePermanentFailure {
		t.Fatalf("permanent failure=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"fail_n_then_success","failures":2}`, 2)); got.Outcome != domain.OutcomeRetryableFailure {
		t.Fatalf("retry=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"fail_n_then_success","failures":2}`, 3)); got.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("later success=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"large_result","result_size":128}`, 1)); len(got.Result) < 128 {
		t.Fatalf("large result bytes=%d", len(got.Result))
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"ignore_context","delay_ms":1}`, 1)); got.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("ignore context=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"delayed_report","delay_ms":1}`, 1)); got.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("delayed report=%+v", got)
	}
	if got := mock.Execute(context.Background(), mockTask(`{"mode":"crash"}`, 1)); got.Outcome != domain.OutcomeRetryableFailure || !crashed {
		t.Fatalf("crash=%+v hook=%v", got, crashed)
	}
	randomTask := mockTask(`{"mode":"random_failure","seed":42}`, 1)
	first := mock.Execute(context.Background(), randomTask)
	second := mock.Execute(context.Background(), randomTask)
	if first.Outcome != second.Outcome {
		t.Fatalf("seeded outcomes differ: %s != %s", first.Outcome, second.Outcome)
	}
}
func TestMockTimeoutHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	got := (&Mock{}).Execute(ctx, mockTask(`{"mode":"timeout"}`, 1))
	if got.Outcome != domain.OutcomeTimeout {
		t.Fatalf("outcome=%s", got.Outcome)
	}
}
