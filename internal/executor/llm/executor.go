package llm

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/domain"
	baseexecutor "github.com/mrussss/orbit-scheduler/internal/executor"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Cost struct {
	PromptMicrounitsPerMillionTokens     int64
	CompletionMicrounitsPerMillionTokens int64
}

type ExecutorConfig struct {
	ProviderName    string
	Provider        Provider
	AllowedModels   []string
	MaxPromptBytes  int64
	MaxOutputTokens int
	MaxConcurrency  int
	CostTable       map[string]Cost
	Observer        Observer
}

type Executor struct {
	providerName string
	provider     Provider
	policy       PayloadPolicy
	semaphore    chan struct{}
	costTable    map[string]Cost
	observer     Observer
}

type ResultContract struct {
	Provider                string `json:"provider"`
	Model                   string `json:"model"`
	Content                 string `json:"content"`
	FinishReason            string `json:"finish_reason"`
	Usage                   Usage  `json:"usage"`
	LatencyMS               int64  `json:"latency_ms"`
	EstimatedCostMicrounits *int64 `json:"estimated_cost_microunits,omitempty"`
}

func NewExecutor(cfg ExecutorConfig) (*Executor, error) {
	if cfg.ProviderName == "" || cfg.Provider == nil || len(cfg.AllowedModels) == 0 || cfg.MaxPromptBytes <= 0 || cfg.MaxOutputTokens <= 0 || cfg.MaxConcurrency <= 0 {
		return nil, errors.New("invalid LLM executor configuration")
	}
	allowed := make(map[string]struct{}, len(cfg.AllowedModels))
	for _, model := range cfg.AllowedModels {
		if model == "" {
			return nil, errors.New("LLM allowed model cannot be empty")
		}
		allowed[model] = struct{}{}
	}
	for model, cost := range cfg.CostTable {
		if _, ok := allowed[model]; !ok || cost.PromptMicrounitsPerMillionTokens < 0 || cost.CompletionMicrounitsPerMillionTokens < 0 {
			return nil, errors.New("invalid LLM cost table")
		}
	}
	observer := cfg.Observer
	if observer == nil {
		observer = noopObserver{}
	}
	return &Executor{
		providerName: cfg.ProviderName,
		provider:     cfg.Provider,
		policy:       PayloadPolicy{AllowedModels: allowed, MaxPromptBytes: cfg.MaxPromptBytes, MaxOutputTokens: cfg.MaxOutputTokens},
		semaphore:    make(chan struct{}, cfg.MaxConcurrency),
		costTable:    cfg.CostTable,
		observer:     observer,
	}, nil
}

func (e *Executor) Execute(ctx context.Context, task scheduler.Assignment) baseexecutor.Result {
	started := time.Now().UTC()
	payload, err := ParsePayload(task.Payload, e.policy)
	if err != nil {
		return executorFailure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "invalid llm payload: "+err.Error())
	}
	modelLabel := payload.Model
	select {
	case e.semaphore <- struct{}{}:
	case <-ctx.Done():
		return contextFailure(started, ctx)
	}
	defer func() { <-e.semaphore }()
	e.observer.Started(e.providerName, modelLabel)
	requestStarted := time.Now()
	response, providerErr := e.provider.Generate(ctx, Request{Model: payload.Model, Messages: payload.Messages, Temperature: payload.Temperature, MaxOutputTokens: payload.MaxOutputTokens, ResponseFormat: payload.ResponseFormat})
	duration := time.Since(requestStarted)
	if providerErr != nil {
		result, rateLimited := mapProviderFailure(started, ctx, providerErr)
		e.observer.Finished(e.providerName, modelLabel, string(result.Outcome), duration.Seconds(), Usage{}, nil, rateLimited)
		return result
	}
	estimatedCost := e.estimatedCost(payload.Model, response.Usage)
	resultPayload := ResultContract{Provider: e.providerName, Model: response.Model, Content: response.Content, FinishReason: response.FinishReason, Usage: response.Usage, LatencyMS: duration.Milliseconds(), EstimatedCostMicrounits: estimatedCost}
	raw, err := json.Marshal(resultPayload)
	if err != nil {
		result := executorFailure(started, domain.OutcomePermanentFailure, domain.ErrorInternal, "failed to encode llm result")
		e.observer.Finished(e.providerName, modelLabel, string(result.Outcome), duration.Seconds(), Usage{}, nil, false)
		return result
	}
	canonical, err := domain.CanonicalJSON(raw)
	if err != nil {
		result := executorFailure(started, domain.OutcomePermanentFailure, domain.ErrorInternal, "failed to canonicalize llm result")
		e.observer.Finished(e.providerName, modelLabel, string(result.Outcome), duration.Seconds(), Usage{}, nil, false)
		return result
	}
	if len(canonical) > domain.MaxResultBytes {
		result := executorFailure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "llm result exceeds Orbit result size limit")
		e.observer.Finished(e.providerName, modelLabel, string(result.Outcome), duration.Seconds(), response.Usage, estimatedCost, false)
		return result
	}
	e.observer.Finished(e.providerName, modelLabel, string(domain.OutcomeSucceeded), duration.Seconds(), response.Usage, estimatedCost, false)
	return baseexecutor.Result{Outcome: domain.OutcomeSucceeded, Result: canonical, ResultHash: domain.HashBytes(canonical), StartedAt: started, FinishedAt: time.Now().UTC()}
}

func mapProviderFailure(started time.Time, ctx context.Context, err error) (baseexecutor.Result, bool) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return executorFailure(started, domain.OutcomeTimeout, domain.ErrorTimeout, "llm request timed out"), false
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return executorFailure(started, domain.OutcomeCanceled, domain.ErrorCanceled, "llm request canceled"), false
	}
	providerError, ok := AsProviderError(err)
	if !ok {
		return executorFailure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, "llm provider request failed"), false
	}
	switch providerError.Kind {
	case ErrorTimeout:
		return executorFailure(started, domain.OutcomeTimeout, domain.ErrorTimeout, providerError.Error()), false
	case ErrorCanceled:
		return executorFailure(started, domain.OutcomeCanceled, domain.ErrorCanceled, providerError.Error()), false
	case ErrorRateLimited:
		return executorFailure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, providerError.Error()), true
	default:
		if providerError.Retryable {
			return executorFailure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, providerError.Error()), false
		}
		return executorFailure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, providerError.Error()), false
	}
}

func contextFailure(started time.Time, ctx context.Context) baseexecutor.Result {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return executorFailure(started, domain.OutcomeTimeout, domain.ErrorTimeout, "llm request timed out while waiting for provider capacity")
	}
	return executorFailure(started, domain.OutcomeCanceled, domain.ErrorCanceled, "llm request canceled while waiting for provider capacity")
}

func executorFailure(started time.Time, outcome domain.TaskOutcome, errorType domain.ErrorType, message string) baseexecutor.Result {
	return baseexecutor.Result{Outcome: outcome, ResultHash: domain.HashBytes(nil), ErrorType: errorType, ErrorMessage: message, StartedAt: started, FinishedAt: time.Now().UTC()}
}

func (e *Executor) estimatedCost(model string, usage Usage) *int64 {
	cost, ok := e.costTable[model]
	if !ok {
		return nil
	}
	prompt, ok := tokenCost(usage.PromptTokens, cost.PromptMicrounitsPerMillionTokens)
	if !ok {
		return nil
	}
	completion, ok := tokenCost(usage.CompletionTokens, cost.CompletionMicrounitsPerMillionTokens)
	if !ok || prompt > math.MaxInt64-completion {
		return nil
	}
	total := prompt + completion
	return &total
}

func tokenCost(tokens int, rate int64) (int64, bool) {
	if tokens < 0 || rate < 0 {
		return 0, false
	}
	value := new(big.Int).Mul(big.NewInt(int64(tokens)), big.NewInt(rate))
	value.Div(value, big.NewInt(1_000_000))
	if !value.IsInt64() {
		return 0, false
	}
	return value.Int64(), true
}
