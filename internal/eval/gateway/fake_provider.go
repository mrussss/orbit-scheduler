package gateway

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/mrussss/orbit-scheduler/internal/executor/agent"
	"github.com/mrussss/orbit-scheduler/internal/executor/llm"
)

// FakeProvider deterministically exercises one real read-only tool round and a
// final structured response. It is intended for CI and never makes HTTP calls.
type FakeProvider struct {
	mu       sync.Mutex
	expected Expected
	calls    int
}

func NewFakeProvider(expected Expected) *FakeProvider { return &FakeProvider{expected: expected} }

func (f *FakeProvider) GenerateWithTools(_ context.Context, request llm.ToolRequest) (llm.ToolResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 {
		return llm.ToolResponse{Model: request.Model, FinishReason: "tool_calls", Usage: llm.Usage{PromptTokens: 20, CompletionTokens: 4, TotalTokens: 24}, ToolCalls: []llm.ToolCall{{ID: "fixture-read", Name: agent.ToolReadFile, Arguments: json.RawMessage(`{"path":"` + f.expected.ExpectedFiles[0] + `","start_line":1,"end_line":200}`)}}}, nil
	}
	diagnosis := agent.Diagnosis{ProblemType: f.expected.ExpectedCategory, LikelyCauses: []string{"The fixed fixture points to the bounded implementation path."}, CodeEvidence: []agent.Evidence{{Path: f.expected.ExpectedFiles[0], Line: 1, Excerpt: strings.Join(f.expected.ExpectedEvidence, " ")}}, RecommendedChecks: []string{"Review the referenced regression test and reproduce the supplied fault fixture."}, Confidence: 0.8, Limits: []string{"Static snapshot analysis only; no live Gateway was accessed."}}
	raw, _ := json.Marshal(diagnosis)
	return llm.ToolResponse{Model: request.Model, Content: string(raw), FinishReason: "stop", Usage: llm.Usage{PromptTokens: 30, CompletionTokens: 15, TotalTokens: 45}}, nil
}
