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
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	MaxCompletionTokens int            `json:"max_completion_tokens"`
	ResponseFormat      map[string]any `json:"response_format,omitempty"`
	Stream              bool           `json:"stream"`
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
	p.mu.Lock()
	p.attempts[content]++
	attempt := p.attempts[content]
	p.mu.Unlock()
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
