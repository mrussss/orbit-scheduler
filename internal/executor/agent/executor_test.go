package agent

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type fakeToolProvider struct {
	mu        sync.Mutex
	responses []llm.ToolResponse
	err       error
	requests  []llm.ToolRequest
	block     bool
	hook      func()
}

func (f *fakeToolProvider) GenerateWithTools(ctx context.Context, request llm.ToolRequest) (llm.ToolResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	index := len(f.requests) - 1
	response := llm.ToolResponse{}
	if len(f.responses) > 0 {
		response = f.responses[index%len(f.responses)]
	}
	err, block, hook := f.err, f.block, f.hook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if block {
		<-ctx.Done()
		return llm.ToolResponse{}, ctx.Err()
	}
	return response, err
}

type memoryTracer struct {
	mu    sync.Mutex
	steps []TraceStep
	err   error
}

func (m *memoryTracer) Record(_ context.Context, step TraceStep) error {
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps = append(m.steps, step)
	return nil
}

func TestExecutorRunsBoundedToolLoopAndProducesStructuredResult(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "queue.go"), "package queue\nfunc push() {}\n")
	provider := &fakeToolProvider{responses: []llm.ToolResponse{
		{Model: "model-a", FinishReason: "tool_calls", Usage: llm.Usage{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12}, ToolCalls: []llm.ToolCall{{ID: "call-1", Name: ToolSearchCode, Arguments: json.RawMessage(`{"query":"push"}`)}}},
		{Model: "model-a", FinishReason: "stop", Usage: llm.Usage{PromptTokens: 20, CompletionTokens: 8, TotalTokens: 28}, Content: `{"problem_type":"queue","likely_causes":["partial write"],"code_evidence":[{"path":"queue.go","line":2,"excerpt":"func push() {}"}],"recommended_checks":["add partial-write test"],"confidence":0.75,"limits":[]}`},
	}}
	tracer := &memoryTracer{}
	executor := newTestExecutor(t, root, provider, tracer, 4)
	result := executor.Execute(context.Background(), agentAssignment())
	if result.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("result=%+v", result)
	}
	var contract ResultContract
	if err := json.Unmarshal(result.Result, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.ProblemType != "queue" || contract.ModelCalls != 2 || contract.ToolCalls != 1 || contract.TotalTokens != 40 || contract.EstimatedCostMicrounits != 50 {
		t.Fatalf("contract=%+v", contract)
	}
	if len(provider.requests) != 2 || len(provider.requests[0].Tools) != 3 || provider.requests[0].Tools[0].Name != ToolSearchCode || provider.requests[0].Tools[1].Name != ToolReadFile || provider.requests[0].Tools[2].Name != ToolReadDocs {
		t.Fatalf("unexpected requests: %+v", provider.requests)
	}
	lastMessages := provider.requests[1].Messages
	if lastMessages[len(lastMessages)-1].Role != "tool" || lastMessages[len(lastMessages)-1].ToolCallID != "call-1" {
		t.Fatalf("tool result not returned to model: %+v", lastMessages)
	}
	previous := 0
	for _, step := range tracer.steps {
		if step.StepNo < previous || len(step.InputSummary)+len(step.OutputSummary) > 2048 {
			t.Fatalf("invalid trace sequence: %+v", tracer.steps)
		}
		previous = step.StepNo
	}
	if tracer.steps[len(tracer.steps)-1].Kind != StepFinal {
		t.Fatalf("missing final trace: %+v", tracer.steps)
	}
}

func TestExecutorRejectsUnknownToolAndExhaustsMaxSteps(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "main.go"), "package main\n")
	t.Run("unknown tool", func(t *testing.T) {
		provider := &fakeToolProvider{responses: []llm.ToolResponse{{Model: "model-a", FinishReason: "tool_calls", Usage: llm.Usage{}, ToolCalls: []llm.ToolCall{{ID: "x", Name: "shell", Arguments: json.RawMessage(`{"command":"id"}`)}}}}}
		result := newTestExecutor(t, root, provider, nil, 3).Execute(context.Background(), agentAssignment())
		if result.Outcome != domain.OutcomePermanentFailure {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("max steps", func(t *testing.T) {
		provider := &fakeToolProvider{responses: []llm.ToolResponse{{Model: "model-a", FinishReason: "tool_calls", Usage: llm.Usage{}, ToolCalls: []llm.ToolCall{{ID: "x", Name: ToolSearchCode, Arguments: json.RawMessage(`{"query":"main"}`)}}}}}
		result := newTestExecutor(t, root, provider, nil, 3).Execute(context.Background(), agentAssignment())
		if result.Outcome != domain.OutcomePermanentFailure || len(provider.requests) != 3 {
			t.Fatalf("result=%+v requests=%d", result, len(provider.requests))
		}
	})
}

func TestExecutorCancellationRateLimitAndTraceFailure(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "main.go"), "package main\n")
	t.Run("cancel", func(t *testing.T) {
		provider := &fakeToolProvider{block: true}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result := newTestExecutor(t, root, provider, nil, 3).Execute(ctx, agentAssignment())
		if result.Outcome != domain.OutcomeCanceled {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("cancel during tool loop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		provider := &fakeToolProvider{hook: cancel, responses: []llm.ToolResponse{{Model: "model-a", FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{ID: "x", Name: ToolSearchCode, Arguments: json.RawMessage(`{"query":"main"}`)}}}}}
		result := newTestExecutor(t, root, provider, nil, 3).Execute(ctx, agentAssignment())
		if result.Outcome != domain.OutcomeCanceled {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("429 retryable", func(t *testing.T) {
		provider := &fakeToolProvider{err: &llm.ProviderError{Kind: llm.ErrorRateLimited, Retryable: true}}
		result := newTestExecutor(t, root, provider, nil, 3).Execute(context.Background(), agentAssignment())
		if result.Outcome != domain.OutcomeRetryableFailure {
			t.Fatalf("result=%+v", result)
		}
	})
	t.Run("trace transport retryable", func(t *testing.T) {
		provider := &fakeToolProvider{}
		tracer := &memoryTracer{err: errors.New("database unavailable")}
		result := newTestExecutor(t, root, provider, tracer, 3).Execute(context.Background(), agentAssignment())
		if result.Outcome != domain.OutcomeRetryableFailure {
			t.Fatalf("result=%+v", result)
		}
	})
}

func TestExecutorTraceNeverPersistsPromptArgumentsOrToolResultContent(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "main.go"), "package main\nconst marker = \"secret-source-marker\"\n")
	provider := &fakeToolProvider{responses: []llm.ToolResponse{
		{Model: "model-a", FinishReason: "tool_calls", ToolCalls: []llm.ToolCall{{ID: "call-secret", Name: ToolReadFile, Arguments: json.RawMessage(`{"path":"main.go"}`)}}},
		{Model: "model-a", FinishReason: "stop", Content: `{"problem_type":"inspection","likely_causes":["bounded"],"code_evidence":[],"recommended_checks":["review"],"confidence":0.5,"limits":[]}`},
	}}
	tracer := &memoryTracer{}
	executor := newTestExecutor(t, root, provider, tracer, 3)
	assignment := agentAssignment()
	assignment.Payload = json.RawMessage(`{"repository_root":"gateway","issue":"secret-issue-marker","error_log":"secret-log-marker"}`)
	if result := executor.Execute(context.Background(), assignment); result.Outcome != domain.OutcomeSucceeded {
		t.Fatalf("result=%+v", result)
	}
	trace, err := json.Marshal(tracer.steps)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-issue-marker", "secret-log-marker", "secret-source-marker", `\"path\":\"main.go\"`} {
		if strings.Contains(string(trace), forbidden) {
			t.Fatalf("trace leaked %q: %s", forbidden, trace)
		}
	}
}

func newTestExecutor(t *testing.T, root string, provider llm.ToolProvider, tracer Tracer, maxSteps int) *Executor {
	t.Helper()
	toolbox := newTestToolbox(t, root, ToolLimits{MaxFileBytes: 4096, MaxResultBytes: 4096, MaxMatches: 10})
	executor, err := NewExecutor(ExecutorConfig{Provider: provider, Toolbox: toolbox, Tracer: tracer, Repositories: map[string]string{"gateway": root}, Model: "model-a", MaxIssueBytes: 4096, MaxOutputTokens: 1024, MaxModelSteps: maxSteps, MaxConcurrency: 2, Cost: Cost{PromptMicrounitsPerMillionTokens: 1_000_000, CompletionMicrounitsPerMillionTokens: 2_000_000}})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func agentAssignment() scheduler.Assignment {
	return scheduler.Assignment{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "agent", AttemptNo: 1, Payload: json.RawMessage(`{"repository_root":"gateway","issue":"queue stalls","error_log":"timeout"}`), ExecutionTimeout: time.Second}
}
