package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeProvider struct {
	response Response
	err      error
	block    <-chan struct{}
	current  atomic.Int32
	maximum  atomic.Int32
	requests atomic.Int32
}

func (p *fakeProvider) Generate(ctx context.Context, _ Request) (Response, error) {
	p.requests.Add(1)
	current := p.current.Add(1)
	defer p.current.Add(-1)
	for {
		maximum := p.maximum.Load()
		if current <= maximum || p.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	if p.block != nil {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()
		case <-p.block:
		}
	}
	return p.response, p.err
}

func TestExecutorBuildsUsageCostAndStableHash(t *testing.T) {
	provider := &fakeProvider{response: Response{Model: "model-a-2026", Content: "answer", FinishReason: "stop", Usage: Usage{PromptTokens: 1_000_000, CompletionTokens: 500_000, TotalTokens: 1_500_000}}}
	executor := newTestExecutor(t, provider, ExecutorConfig{CostTable: map[string]Cost{"model-a": {PromptMicrounitsPerMillionTokens: 10, CompletionMicrounitsPerMillionTokens: 20}}})
	result := executor.Execute(context.Background(), llmAssignment())
	if result.Outcome != domain.OutcomeSucceeded || result.ErrorMessage != "" {
		t.Fatalf("result=%+v", result)
	}
	if result.ResultHash != domain.HashBytes(result.Result) {
		t.Fatal("result hash does not match canonical result")
	}
	var contract ResultContract
	if err := json.Unmarshal(result.Result, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Provider != "openai-compatible" || contract.Model != "model-a-2026" || contract.Content != "answer" || contract.EstimatedCostMicrounits == nil || *contract.EstimatedCostMicrounits != 20 {
		t.Fatalf("contract=%+v", contract)
	}
}

func TestExecutorOmitsUnknownCost(t *testing.T) {
	executor := newTestExecutor(t, &fakeProvider{response: Response{Model: "model-a", Content: "answer"}}, ExecutorConfig{})
	result := executor.Execute(context.Background(), llmAssignment())
	var contract map[string]any
	if err := json.Unmarshal(result.Result, &contract); err != nil {
		t.Fatal(err)
	}
	if _, exists := contract["estimated_cost_microunits"]; exists {
		t.Fatal("executor fabricated cost without a configured cost entry")
	}
}

func TestExecutorRejectsResultAboveReportingLimit(t *testing.T) {
	provider := &fakeProvider{response: Response{Model: "model-a", Content: strings.Repeat("x", domain.MaxResultBytes), FinishReason: "stop", Usage: Usage{TotalTokens: 1}}}
	executor := newTestExecutor(t, provider, ExecutorConfig{})
	result := executor.Execute(context.Background(), llmAssignment())
	if result.Outcome != domain.OutcomePermanentFailure || result.ErrorType != domain.ErrorExecutor || len(result.Result) != 0 {
		t.Fatalf("result=%+v result_bytes=%d", result, len(result.Result))
	}
}

func TestExecutorMapsProviderErrors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		outcome   domain.TaskOutcome
		errorType domain.ErrorType
	}{
		{"rate limit", &ProviderError{Kind: ErrorRateLimited, Retryable: true, StatusCode: 429}, domain.OutcomeRetryableFailure, domain.ErrorTransport},
		{"upstream", &ProviderError{Kind: ErrorUpstream, Retryable: true, StatusCode: 503}, domain.OutcomeRetryableFailure, domain.ErrorTransport},
		{"invalid", &ProviderError{Kind: ErrorInvalidRequest, StatusCode: 400}, domain.OutcomePermanentFailure, domain.ErrorExecutor},
		{"auth", &ProviderError{Kind: ErrorAuthentication, StatusCode: 401}, domain.OutcomePermanentFailure, domain.ErrorExecutor},
		{"too large", &ProviderError{Kind: ErrorResponseTooLarge, StatusCode: 200}, domain.OutcomePermanentFailure, domain.ErrorExecutor},
		{"timeout", &ProviderError{Kind: ErrorTimeout, Retryable: true}, domain.OutcomeTimeout, domain.ErrorTimeout},
		{"unknown", errors.New("unknown"), domain.OutcomeRetryableFailure, domain.ErrorTransport},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := newTestExecutor(t, &fakeProvider{err: test.err}, ExecutorConfig{}).Execute(context.Background(), llmAssignment())
			if result.Outcome != test.outcome || result.ErrorType != test.errorType {
				t.Fatalf("result=%+v", result)
			}
			if result.ResultHash != domain.HashBytes(nil) {
				t.Fatal("failure result hash is not empty hash")
			}
		})
	}
}

func TestExecutorRejectsInvalidPayloadBeforeProvider(t *testing.T) {
	provider := &fakeProvider{}
	executor := newTestExecutor(t, provider, ExecutorConfig{})
	task := llmAssignment()
	task.Payload = []byte(`{"model":"unapproved","messages":[{"role":"user","content":"hello"}],"max_output_tokens":20}`)
	result := executor.Execute(context.Background(), task)
	if result.Outcome != domain.OutcomePermanentFailure || provider.requests.Load() != 0 {
		t.Fatalf("result=%+v provider requests=%d", result, provider.requests.Load())
	}
}

func TestExecutorBoundsProviderConcurrencyAndCancelsWaiter(t *testing.T) {
	release := make(chan struct{})
	provider := &fakeProvider{response: Response{Model: "model-a", Content: "answer"}, block: release}
	executor := newTestExecutor(t, provider, ExecutorConfig{MaxConcurrency: 2})
	var wait sync.WaitGroup
	results := make(chan domain.TaskOutcome, 3)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- executor.Execute(context.Background(), llmAssignment()).Outcome
		}()
	}
	deadline := time.Now().Add(time.Second)
	for provider.current.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if provider.maximum.Load() != 2 {
		t.Fatalf("provider concurrency=%d", provider.maximum.Load())
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if result := executor.Execute(waitCtx, llmAssignment()); result.Outcome != domain.OutcomeTimeout {
		t.Fatalf("waiting outcome=%s", result.Outcome)
	}
	if provider.requests.Load() != 2 {
		t.Fatalf("provider requests=%d", provider.requests.Load())
	}
	close(release)
	wait.Wait()
	close(results)
	for outcome := range results {
		if outcome != domain.OutcomeSucceeded {
			t.Fatalf("outcome=%s", outcome)
		}
	}
}

func TestExecutorPrometheusMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer, err := NewPrometheusObserver(registry)
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeProvider{response: Response{Model: "model-a", Content: "answer", Usage: Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}}
	executor := newTestExecutor(t, provider, ExecutorConfig{Observer: observer, CostTable: map[string]Cost{"model-a": {PromptMicrounitsPerMillionTokens: 1_000_000, CompletionMicrounitsPerMillionTokens: 1_000_000}}})
	if result := executor.Execute(context.Background(), llmAssignment()); result.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("result=%+v", result)
	}
	if got := testutil.ToFloat64(observer.requests.WithLabelValues("openai-compatible", "model-a", string(domain.OutcomeSucceeded))); got != 1 {
		t.Fatalf("requests=%f", got)
	}
	if got := testutil.ToFloat64(observer.tokens.WithLabelValues("openai-compatible", "model-a", "prompt")); got != 3 {
		t.Fatalf("prompt tokens=%f", got)
	}
	if got := testutil.ToFloat64(observer.cost.WithLabelValues("openai-compatible", "model-a")); got != 5 {
		t.Fatalf("cost=%f", got)
	}
	if got := testutil.ToFloat64(observer.inFlight.WithLabelValues("openai-compatible", "model-a")); got != 0 {
		t.Fatalf("in flight=%f", got)
	}
	provider.err = &ProviderError{Kind: ErrorRateLimited, StatusCode: 429, Retryable: true}
	if result := executor.Execute(context.Background(), llmAssignment()); result.Outcome != domain.OutcomeRetryableFailure {
		t.Fatalf("rate-limited result=%+v", result)
	}
	if got := testutil.ToFloat64(observer.rateLimited.WithLabelValues("openai-compatible", "model-a")); got != 1 {
		t.Fatalf("rate limited=%f", got)
	}
	if got := testutil.ToFloat64(observer.requests.WithLabelValues("openai-compatible", "model-a", string(domain.OutcomeRetryableFailure))); got != 1 {
		t.Fatalf("retryable requests=%f", got)
	}
}

func newTestExecutor(t *testing.T, provider Provider, overrides ExecutorConfig) *Executor {
	t.Helper()
	cfg := ExecutorConfig{ProviderName: "openai-compatible", Provider: provider, AllowedModels: []string{"model-a"}, MaxPromptBytes: 1024, MaxOutputTokens: 100, MaxConcurrency: 1}
	if overrides.CostTable != nil {
		cfg.CostTable = overrides.CostTable
	}
	if overrides.Observer != nil {
		cfg.Observer = overrides.Observer
	}
	if overrides.MaxConcurrency > 0 {
		cfg.MaxConcurrency = overrides.MaxConcurrency
	}
	executor, err := NewExecutor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func llmAssignment() scheduler.Assignment {
	return scheduler.Assignment{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "llm", Payload: []byte(`{"model":"model-a","messages":[{"role":"user","content":"hello"}],"max_output_tokens":20}`), AttemptNo: 1, ExecutionTimeout: time.Second}
}
