package agent

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/domain"
	baseexecutor "github.com/mrussss/orbit-scheduler/internal/executor"
	"github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Cost struct {
	PromptMicrounitsPerMillionTokens     int64
	CompletionMicrounitsPerMillionTokens int64
}

type ExecutorConfig struct {
	Provider        llm.ToolProvider
	Toolbox         *Toolbox
	Tracer          Tracer
	Repositories    map[string]string
	Model           string
	MaxIssueBytes   int
	MaxOutputTokens int
	MaxModelSteps   int
	MaxToolCalls    int
	MaxConcurrency  int
	Cost            Cost
}

type Executor struct {
	provider        llm.ToolProvider
	toolbox         *Toolbox
	tracer          Tracer
	repositories    map[string]string
	model           string
	maxIssueBytes   int
	maxOutputTokens int
	maxModelSteps   int
	maxToolCalls    int
	semaphore       chan struct{}
	cost            Cost
}

func NewExecutor(cfg ExecutorConfig) (*Executor, error) {
	if cfg.Provider == nil || cfg.Toolbox == nil || len(cfg.Repositories) == 0 || cfg.Model == "" || cfg.MaxOutputTokens <= 0 || cfg.MaxModelSteps < 3 || cfg.MaxModelSteps > 6 || cfg.MaxConcurrency <= 0 || cfg.Cost.PromptMicrounitsPerMillionTokens < 0 || cfg.Cost.CompletionMicrounitsPerMillionTokens < 0 {
		return nil, errors.New("invalid agent executor configuration")
	}
	if cfg.MaxToolCalls <= 0 {
		cfg.MaxToolCalls = cfg.MaxModelSteps * 3
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = noopTracer{}
	}
	return &Executor{provider: cfg.Provider, toolbox: cfg.Toolbox, tracer: tracer, repositories: cfg.Repositories, model: cfg.Model, maxIssueBytes: cfg.MaxIssueBytes, maxOutputTokens: cfg.MaxOutputTokens, maxModelSteps: cfg.MaxModelSteps, maxToolCalls: cfg.MaxToolCalls, semaphore: make(chan struct{}, cfg.MaxConcurrency), cost: cfg.Cost}, nil
}

func (e *Executor) Execute(ctx context.Context, task scheduler.Assignment) baseexecutor.Result {
	started := time.Now().UTC()
	payload, err := ParsePayload(task.Payload, e.repositories, e.maxIssueBytes)
	if err != nil {
		return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "invalid agent payload: "+err.Error())
	}
	select {
	case e.semaphore <- struct{}{}:
	case <-ctx.Done():
		return contextFailure(started, ctx)
	}
	defer func() { <-e.semaphore }()

	messages := []llm.ToolMessage{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: userPrompt(payload)},
	}
	definitions := ToolDefinitions()
	var usage llm.Usage
	var modelCalls, toolCalls, stepNo int
	for modelRound := 1; modelRound <= e.maxModelSteps; modelRound++ {
		if err := ctx.Err(); err != nil {
			return contextFailure(started, ctx)
		}
		stepNo++
		modelStarted := time.Now().UTC()
		modelStep := TraceStep{TaskID: task.TaskID, AttemptNo: task.AttemptNo, StepNo: stepNo, Kind: StepModel, InputSummary: summary(map[string]any{"message_count": len(messages), "tool_count": len(definitions), "round": modelRound}), Status: StepRunning, StartedAt: modelStarted}
		if err := e.tracer.Record(ctx, modelStep); err != nil {
			return traceFailure(started, ctx, err)
		}
		response, providerErr := e.provider.GenerateWithTools(ctx, llm.ToolRequest{Model: e.model, Messages: messages, Tools: definitions, MaxOutputTokens: e.maxOutputTokens})
		modelCalls++
		if providerErr != nil {
			finished := time.Now().UTC()
			modelStep.Status, modelStep.FinishedAt, modelStep.OutputSummary = StepFailed, &finished, summary(map[string]any{"error": "provider_request_failed"})
			_ = e.tracer.Record(context.WithoutCancel(ctx), modelStep)
			result := providerFailure(started, ctx, providerErr)
			_ = e.recordTerminal(task, &stepNo, StepError, result.ErrorMessage, false)
			return result
		}
		usage.PromptTokens += response.Usage.PromptTokens
		usage.CompletionTokens += response.Usage.CompletionTokens
		usage.TotalTokens += response.Usage.TotalTokens
		finished := time.Now().UTC()
		modelStep.Status, modelStep.FinishedAt, modelStep.OutputSummary = StepSucceeded, &finished, summary(map[string]any{"finish_reason": response.FinishReason, "tool_calls": len(response.ToolCalls), "prompt_tokens": response.Usage.PromptTokens, "completion_tokens": response.Usage.CompletionTokens})
		if err := e.tracer.Record(ctx, modelStep); err != nil {
			return traceFailure(started, ctx, err)
		}

		if len(response.ToolCalls) == 0 {
			diagnosis, err := ParseDiagnosis(response.Content)
			if err != nil {
				result := failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "invalid agent final response: "+err.Error())
				_ = e.recordTerminal(task, &stepNo, StepError, "invalid_final_response", false)
				return result
			}
			contract := ResultContract{Diagnosis: diagnosis, Model: response.Model, PromptTokens: usage.PromptTokens, CompletionTokens: usage.CompletionTokens, TotalTokens: usage.TotalTokens, EstimatedCostMicrounits: estimateCost(usage, e.cost), ModelCalls: modelCalls, ToolCalls: toolCalls, LatencyMS: time.Since(started).Milliseconds()}
			raw, err := json.Marshal(contract)
			if err != nil {
				return failure(started, domain.OutcomePermanentFailure, domain.ErrorInternal, "failed to encode agent result")
			}
			canonical, err := domain.CanonicalJSON(raw)
			if err != nil || len(canonical) > domain.MaxResultBytes {
				return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "agent result exceeds result contract")
			}
			if err := e.recordTerminal(task, &stepNo, StepFinal, "diagnosis_ready", true); err != nil {
				return traceFailure(started, ctx, err)
			}
			return baseexecutor.Result{Outcome: domain.OutcomeSucceeded, Result: canonical, ResultHash: domain.HashBytes(canonical), StartedAt: started, FinishedAt: time.Now().UTC()}
		}

		messages = append(messages, llm.ToolMessage{Role: "assistant", Content: response.Content, ToolCalls: response.ToolCalls})
		for _, call := range response.ToolCalls {
			toolCalls++
			if toolCalls > e.maxToolCalls {
				result := failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "agent tool call limit exhausted")
				_ = e.recordTerminal(task, &stepNo, StepError, "tool_call_limit", false)
				return result
			}
			stepNo++
			toolStarted := time.Now().UTC()
			toolStep := TraceStep{TaskID: task.TaskID, AttemptNo: task.AttemptNo, StepNo: stepNo, Kind: StepTool, ToolName: call.Name, InputSummary: summary(map[string]any{"arguments_bytes": len(call.Arguments), "repository": payload.RepositoryRoot}), Status: StepRunning, StartedAt: toolStarted}
			if err := e.tracer.Record(ctx, toolStep); err != nil {
				return traceFailure(started, ctx, err)
			}
			toolResult, toolErr := e.toolbox.Execute(ctx, payload.RepositoryRoot, call.Name, call.Arguments)
			toolFinished := time.Now().UTC()
			if toolErr != nil {
				toolStep.Status, toolStep.FinishedAt, toolStep.OutputSummary = StepFailed, &toolFinished, summary(map[string]any{"error": "tool_rejected"})
				_ = e.tracer.Record(context.WithoutCancel(ctx), toolStep)
				if ctx.Err() != nil {
					result := contextFailure(started, ctx)
					_ = e.recordTerminal(task, &stepNo, StepError, "tool_canceled", false)
					return result
				}
				result := failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "agent tool call rejected")
				_ = e.recordTerminal(task, &stepNo, StepError, "tool_rejected", false)
				return result
			}
			toolStep.Status, toolStep.FinishedAt, toolStep.OutputSummary = StepSucceeded, &toolFinished, summary(map[string]any{"result_bytes": len(toolResult)})
			if err := e.tracer.Record(ctx, toolStep); err != nil {
				return traceFailure(started, ctx, err)
			}
			messages = append(messages, llm.ToolMessage{Role: "tool", ToolCallID: call.ID, Content: string(toolResult)})
		}
	}
	result := failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, "agent max model steps exhausted")
	_ = e.recordTerminal(task, &stepNo, StepError, "max_steps_exhausted", false)
	return result
}

func (e *Executor) recordTerminal(task scheduler.Assignment, stepNo *int, kind StepKind, reason string, succeeded bool) error {
	*stepNo++
	now := time.Now().UTC()
	status := StepFailed
	if succeeded {
		status = StepSucceeded
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return e.tracer.Record(ctx, TraceStep{TaskID: task.TaskID, AttemptNo: task.AttemptNo, StepNo: *stepNo, Kind: kind, OutputSummary: summary(map[string]any{"reason": reason}), Status: status, StartedAt: now, FinishedAt: &now})
}

func userPrompt(payload Payload) string {
	return "Repository alias: " + payload.RepositoryRoot + "\nIssue:\n" + payload.Issue + "\nError log:\n" + payload.ErrorLog
}

const systemPrompt = `You diagnose issues in an allowlisted source snapshot. Use only search_code, read_file, and read_docs. Never claim to run shell commands, change files, access a live system, or see evidence that tools did not return. Return one JSON object with exactly: problem_type (string), likely_causes (string array), code_evidence (array of {path,line,excerpt}), recommended_checks (string array), confidence (number 0..1), limits (string array).`

func summary(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

func traceFailure(started time.Time, ctx context.Context, err error) baseexecutor.Result {
	if ctx.Err() != nil || errors.Is(err, scheduler.ErrStaleLease) {
		return contextFailure(started, ctx)
	}
	return failure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, "agent trace write failed")
}

func providerFailure(started time.Time, ctx context.Context, err error) baseexecutor.Result {
	if ctx.Err() != nil {
		return contextFailure(started, ctx)
	}
	providerError, ok := llm.AsProviderError(err)
	if !ok || providerError.Retryable {
		return failure(started, domain.OutcomeRetryableFailure, domain.ErrorTransport, "agent provider request failed")
	}
	return failure(started, domain.OutcomePermanentFailure, domain.ErrorExecutor, providerError.Error())
}

func contextFailure(started time.Time, ctx context.Context) baseexecutor.Result {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return failure(started, domain.OutcomeTimeout, domain.ErrorTimeout, "agent execution timed out")
	}
	return failure(started, domain.OutcomeCanceled, domain.ErrorCanceled, "agent execution canceled")
}

func failure(started time.Time, outcome domain.TaskOutcome, errorType domain.ErrorType, message string) baseexecutor.Result {
	return baseexecutor.Result{Outcome: outcome, ResultHash: domain.HashBytes(nil), ErrorType: errorType, ErrorMessage: message, StartedAt: started, FinishedAt: time.Now().UTC()}
}

func estimateCost(usage llm.Usage, cost Cost) int64 {
	prompt := tokenCost(usage.PromptTokens, cost.PromptMicrounitsPerMillionTokens)
	completion := tokenCost(usage.CompletionTokens, cost.CompletionMicrounitsPerMillionTokens)
	if prompt > math.MaxInt64-completion {
		return math.MaxInt64
	}
	return prompt + completion
}

func tokenCost(tokens int, rate int64) int64 {
	value := new(big.Int).Mul(big.NewInt(int64(tokens)), big.NewInt(rate))
	value.Div(value, big.NewInt(1_000_000))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}

var _ baseexecutor.Executor = (*Executor)(nil)
