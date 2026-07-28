package domain

import (
	"testing"
	"time"
)

type fixedJitter int64

func (f fixedJitter) Int63n(n int64) int64 {
	if int64(f) >= n {
		return n - 1
	}
	return int64(f)
}

func TestTaskTransitions(t *testing.T) {
	if !CanTransition(TaskPending, TaskRunning) || !CanTransition(TaskRunning, TaskSucceeded) {
		t.Fatal("valid transition rejected")
	}
	for _, terminal := range []TaskStatus{TaskSucceeded, TaskFailed, TaskCanceled} {
		if CanTransition(terminal, TaskPending) || !terminal.Terminal() {
			t.Fatalf("terminal state %s regressed", terminal)
		}
	}
}

func TestCanonicalHashIgnoresObjectOrder(t *testing.T) {
	a, err := HashJSON([]byte(`{"a":1,"b":{"x":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := HashJSON([]byte(`{ "b": { "x": true }, "a": 1 }`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("semantic equivalents produced different hashes")
	}
}

func TestRetryDelayIsCapped(t *testing.T) {
	if got := RetryDelay(2, time.Second, 10*time.Second, fixedJitter(100*time.Millisecond)); got != 2100*time.Millisecond {
		t.Fatalf("delay = %s", got)
	}
	if got := RetryDelay(100, time.Second, 10*time.Second, fixedJitter(1)); got != 10*time.Second {
		t.Fatalf("capped delay = %s", got)
	}
}

func TestDeriveJobStatus(t *testing.T) {
	if got := DeriveJobStatus(JobCounts{Total: 2, Succeeded: 1, Failed: 1}); got != JobPartial {
		t.Fatalf("status = %s", got)
	}
}
