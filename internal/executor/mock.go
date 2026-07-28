package executor

import (
	"context"
	"encoding/json"
	"math/rand"
	"strings"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Mock struct{ CrashHook func(scheduler.Assignment) }
type mockPayload struct {
	Mode       string          `json:"mode"`
	DelayMS    int64           `json:"delay_ms"`
	Failures   int             `json:"failures"`
	Result     json.RawMessage `json:"result"`
	ResultSize int             `json:"result_size"`
	Seed       int64           `json:"seed"`
}

func (m *Mock) Execute(ctx context.Context, task scheduler.Assignment) Result {
	started := time.Now().UTC()
	var payload mockPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "invalid mock payload: "+err.Error())
	}
	if payload.Mode == "" {
		payload.Mode = "success"
	}
	wait := func(respect bool) bool {
		duration := time.Duration(payload.DelayMS) * time.Millisecond
		if duration <= 0 {
			return true
		}
		if !respect {
			time.Sleep(duration)
			return true
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	switch payload.Mode {
	case "success":
		return success(started, payload.Result)
	case "delay_success":
		if !wait(true) {
			return canceledResult(started, ctx)
		}
		return success(started, payload.Result)
	case "permanent_failure":
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "mock permanent failure")
	case "fail_n_then_success":
		if task.AttemptNo <= payload.Failures {
			return failure(started, domain.OutcomeRetryableFailure, domain.ErrorExecutor, "mock retryable failure")
		}
		return success(started, payload.Result)
	case "timeout":
		<-ctx.Done()
		return canceledResult(started, ctx)
	case "ignore_context":
		wait(false)
		return success(started, payload.Result)
	case "delayed_report":
		if !wait(true) {
			return canceledResult(started, ctx)
		}
		return success(started, payload.Result)
	case "crash":
		if m.CrashHook != nil {
			m.CrashHook(task)
		}
		return failure(started, domain.OutcomeRetryableFailure, domain.ErrorInternal, "simulated worker crash")
	case "large_result":
		if payload.ResultSize < 0 {
			payload.ResultSize = 0
		}
		result, _ := json.Marshal(strings.Repeat("x", payload.ResultSize))
		return resultSuccess(started, result)
	case "random_failure":
		source := rand.New(rand.NewSource(payload.Seed + int64(task.AttemptNo)))
		if source.Intn(2) == 0 {
			return failure(started, domain.OutcomeRetryableFailure, domain.ErrorExecutor, "seeded random failure")
		}
		return success(started, payload.Result)
	default:
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "unknown mock mode")
	}
}
func success(started time.Time, raw json.RawMessage) Result {
	if len(raw) == 0 {
		raw = json.RawMessage(`{"ok":true}`)
	}
	canonical, err := domain.CanonicalJSON(raw)
	if err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "mock result is not valid JSON")
	}
	return resultSuccess(started, canonical)
}
func resultSuccess(started time.Time, result json.RawMessage) Result {
	return Result{Outcome: domain.OutcomeSucceeded, Result: result, ResultHash: domain.HashBytes(result), StartedAt: started, FinishedAt: time.Now().UTC()}
}
func failure(started time.Time, outcome domain.TaskOutcome, errorType domain.ErrorType, message string) Result {
	return Result{Outcome: outcome, ResultHash: domain.HashBytes(nil), ErrorType: errorType, ErrorMessage: message, StartedAt: started, FinishedAt: time.Now().UTC()}
}
func canceledResult(started time.Time, ctx context.Context) Result {
	if ctx.Err() == context.DeadlineExceeded {
		return failure(started, domain.OutcomeTimeout, domain.ErrorTimeout, "execution deadline exceeded")
	}
	return failure(started, domain.OutcomeCanceled, domain.ErrorCanceled, "execution canceled")
}
