package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type requestPayload struct {
	Model       string   `json:"model"`
	Temperature *float64 `json:"temperature,omitempty"`
	Messages    []struct {
		Role       string `json:"role"`
		Content    string `json:"content,omitempty"`
		ToolCallID string `json:"tool_call_id,omitempty"`
		ToolCalls  []struct {
			ID       string `json:"id"`
			Type     string `json:"type"`
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		} `json:"tool_calls,omitempty"`
	} `json:"messages"`
	MaxCompletionTokens int            `json:"max_completion_tokens"`
	ResponseFormat      map[string]any `json:"response_format,omitempty"`
	Tools               []struct {
		Type     string `json:"type"`
		Function struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
		} `json:"function"`
	} `json:"tools,omitempty"`
	ToolChoice string `json:"tool_choice,omitempty"`
	Stream     bool   `json:"stream"`
}

type fakeProvider struct {
	apiKey   string
	mu       sync.Mutex
	attempts map[string]int
}

func main() {
	address := env("FAKE_LLM_ADDR", "127.0.0.1:18089")
	provider := &fakeProvider{apiKey: env("FAKE_LLM_API_KEY", "fake-provider-key"), attempts: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", provider.complete)
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatal(err)
		}
	}
}

func (p *fakeProvider) complete(writer http.ResponseWriter, request *http.Request) {
	if request.Header.Get("Authorization") != "Bearer "+p.apiKey {
		writeError(writer, http.StatusUnauthorized)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var payload requestPayload
	if err := decoder.Decode(&payload); err != nil || payload.Model == "" || len(payload.Messages) == 0 || payload.MaxCompletionTokens <= 0 || payload.Stream {
		writeError(writer, http.StatusBadRequest)
		return
	}
	content := payload.Messages[len(payload.Messages)-1].Content
	conversation := strings.Builder{}
	for _, message := range payload.Messages {
		conversation.WriteString(message.Content)
		conversation.WriteByte('\n')
	}
	scenario := agentScenario(conversation.String())
	key := content
	if scenario != "" {
		key = scenario
	}
	p.mu.Lock()
	p.attempts[key]++
	attempt := p.attempts[key]
	p.mu.Unlock()
	if len(payload.Tools) > 0 {
		p.completeAgent(writer, request, payload, scenario, attempt)
		return
	}
	switch {
	case strings.Contains(content, "retry-once") && attempt == 1:
		writeError(writer, http.StatusTooManyRequests)
		return
	case strings.Contains(content, "server-error"):
		writeError(writer, http.StatusServiceUnavailable)
		return
	case strings.Contains(content, "bad-request"):
		writeError(writer, http.StatusBadRequest)
		return
	case strings.Contains(content, "invalid-json"):
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{`))
		return
	case strings.Contains(content, "slow-once") && attempt == 1:
		select {
		case <-request.Context().Done():
			return
		case <-time.After(10 * time.Second):
		}
	case strings.Contains(content, "slow") && !strings.Contains(content, "slow-once"):
		select {
		case <-request.Context().Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"model":   payload.Model,
		"choices": []any{map[string]any{"message": map[string]any{"content": "fake response: " + content}, "finish_reason": "stop"}},
		"usage":   map[string]any{"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
	})
}

func (p *fakeProvider) completeAgent(writer http.ResponseWriter, request *http.Request, payload requestPayload, scenario string, call int) {
	if len(payload.Tools) != 3 || payload.ToolChoice != "auto" {
		writeError(writer, http.StatusBadRequest)
		return
	}
	if strings.Contains(scenario, "agent-retry-once") && call == 1 {
		writeError(writer, http.StatusTooManyRequests)
		return
	}
	lastRole := payload.Messages[len(payload.Messages)-1].Role
	if lastRole == "tool" && ((strings.Contains(scenario, "agent-slow-cancel") && call == 2) || (strings.Contains(scenario, "agent-crash-after-tool") && call == 2)) {
		select {
		case <-request.Context().Done():
			return
		case <-time.After(20 * time.Second):
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	if lastRole != "tool" {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"model": payload.Model,
			"choices": []any{
				map[string]any{
					"message": map[string]any{
						"content": "",
						"tool_calls": []any{
							map[string]any{"id": "call-readme", "type": "function", "function": map[string]any{"name": "read_file", "arguments": `{"path":"README.md","start_line":1,"end_line":20}`}},
						},
					},
					"finish_reason": "tool_calls",
				},
			},
			"usage": map[string]any{"prompt_tokens": 6, "completion_tokens": 3, "total_tokens": 9},
		})
		return
	}
	diagnosis := map[string]any{
		"problem_type":       "repository_diagnosis",
		"likely_causes":      []string{"The supplied fixture requires inspection of the repository contract."},
		"code_evidence":      []any{map[string]any{"path": "README.md", "line": 1, "excerpt": "# Orbit Scheduler"}},
		"recommended_checks": []string{"Review the referenced regression test."},
		"confidence":         0.75,
		"limits":             []string{"Static snapshot analysis only."},
	}
	diagnosisJSON, _ := json.Marshal(diagnosis)
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"model":   payload.Model,
		"choices": []any{map[string]any{"message": map[string]any{"content": string(diagnosisJSON)}, "finish_reason": "stop"}},
		"usage":   map[string]any{"prompt_tokens": 6, "completion_tokens": 3, "total_tokens": 9},
	})
}

func agentScenario(conversation string) string {
	for _, marker := range []string{"agent-retry-once", "agent-slow-cancel", "agent-crash-after-tool", "agent-basic"} {
		if strings.Contains(conversation, marker) {
			return marker
		}
	}
	return ""
}

func writeError(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]any{"message": http.StatusText(status)}})
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
