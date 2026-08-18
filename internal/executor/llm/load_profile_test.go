package llm

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type delayedProvider struct {
	delay            time.Duration
	current, maximum atomic.Int32
}

func (p *delayedProvider) Generate(ctx context.Context, request Request) (Response, error) {
	current := p.current.Add(1)
	defer p.current.Add(-1)
	for {
		maximum := p.maximum.Load()
		if current <= maximum || p.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	timer := time.NewTimer(p.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case <-timer.C:
	}
	return Response{Model: request.Model, Content: "ok", FinishReason: "stop", Usage: Usage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3}}, nil
}

func TestExecutorLoadProfile(t *testing.T) {
	const requests = 200
	provider := &delayedProvider{delay: 2 * time.Millisecond}
	executor, err := NewExecutor(ExecutorConfig{ProviderName: "fake", Provider: provider, AllowedModels: []string{"model-a"}, MaxPromptBytes: 1024, MaxOutputTokens: 100, MaxConcurrency: 8})
	if err != nil {
		t.Fatal(err)
	}
	assignment := scheduler.Assignment{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "llm", Payload: []byte(`{"model":"model-a","messages":[{"role":"user","content":"load profile"}],"max_output_tokens":20}`), AttemptNo: 1, ExecutionTimeout: time.Second}
	durations := make([]time.Duration, requests)
	var failures atomic.Int32
	var wait sync.WaitGroup
	started := time.Now()
	for index := range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			requestStarted := time.Now()
			result := executor.Execute(context.Background(), assignment)
			durations[index] = time.Since(requestStarted)
			if result.Outcome != domain.OutcomeSucceeded {
				failures.Add(1)
			}
		}()
	}
	wait.Wait()
	elapsed := time.Since(started)
	if failures.Load() != 0 {
		t.Fatalf("failures=%d", failures.Load())
	}
	if maximum := provider.maximum.Load(); maximum <= 1 || maximum > 8 {
		t.Fatalf("provider maximum concurrency=%d", maximum)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("requests=%d provider_delay=%s max_concurrency=%d elapsed=%s p50=%s p95=%s p99=%s error_rate=0", requests, provider.delay, provider.maximum.Load(), elapsed, percentile(durations, 50), percentile(durations, 95), percentile(durations, 99))
}

func percentile(sorted []time.Duration, value int) time.Duration {
	index := (len(sorted)*value + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(sorted) {
		index = len(sorted)
	}
	return sorted[index-1]
}
