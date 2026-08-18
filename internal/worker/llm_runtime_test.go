package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/executor"
	llmexecutor "github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestRuntimeExecutesAndReportsLLMTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"model":"model-a","choices":[{"message":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`)
	}))
	defer server.Close()
	runtime, client := newLLMRuntime(t, server.URL)
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(root); err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-client.reports:
		if report.Outcome != domain.OutcomeSucceeded {
			t.Fatalf("outcome=%s error=%s", report.Outcome, report.ErrorMessage)
		}
		var result llmexecutor.ResultContract
		if err := json.Unmarshal(report.Result, &result); err != nil {
			t.Fatal(err)
		}
		if result.Content != "answer" || result.Usage.TotalTokens != 6 {
			t.Fatalf("result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("LLM task was not reported")
	}
	stopCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := runtime.StopNow(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestGracefulShutdownCancelsLLMRequestAndRequeuesOutcome(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	provider := cancelAwareProvider{started: requestStarted, canceled: requestCanceled}
	runtime, client := newLLMRuntimeWithProvider(t, provider)
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("provider request did not start")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := runtime.GracefulShutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("provider request context was not canceled")
	}
	select {
	case report := <-client.reports:
		if report.Outcome != domain.OutcomeRetryableFailure || report.ErrorType != domain.ErrorCanceled {
			t.Fatalf("shutdown report=%+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupted LLM task was not reported")
	}
}

type cancelAwareProvider struct {
	started, canceled chan struct{}
}

func (p cancelAwareProvider) Generate(ctx context.Context, _ llmexecutor.Request) (llmexecutor.Response, error) {
	close(p.started)
	<-ctx.Done()
	close(p.canceled)
	return llmexecutor.Response{}, ctx.Err()
}

func newLLMRuntime(t *testing.T, baseURL string) (*Runtime, *fakeClient) {
	t.Helper()
	provider, err := llmexecutor.NewOpenAICompatible(llmexecutor.OpenAICompatibleConfig{BaseURL: baseURL, APIKey: "test-secret", RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxResponseBytes: 1 << 20, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(provider.CloseIdleConnections)
	return newLLMRuntimeWithProvider(t, provider)
}

func newLLMRuntimeWithProvider(t *testing.T, provider llmexecutor.Provider) (*Runtime, *fakeClient) {
	t.Helper()
	llm, err := llmexecutor.NewExecutor(llmexecutor.ExecutorConfig{ProviderName: "openai-compatible", Provider: provider, AllowedModels: []string{"model-a"}, MaxPromptBytes: 1024, MaxOutputTokens: 100, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	registry := executor.NewRegistry()
	if err := registry.Register("llm", llm); err != nil {
		t.Fatal(err)
	}
	instanceID := uuid.New()
	client := &fakeClient{tasks: []scheduler.Assignment{{TaskID: uuid.New(), ProjectID: uuid.New(), TaskType: "llm", Payload: []byte(`{"model":"model-a","messages":[{"role":"user","content":"hello"}],"max_output_tokens":20}`), AttemptNo: 1, LeaseExpiresAt: time.Now().Add(time.Second), ExecutionTimeout: time.Second}}, reports: make(chan scheduler.ReportRequest, 1)}
	runtime, err := NewRuntime(client, registry, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Registration: Registration{WorkerName: "llm-test", InstanceID: instanceID, Hostname: "test", Capacity: 1, TaskTypes: []string{"llm"}}, LeaseDuration: time.Second, RenewInterval: 100 * time.Millisecond, FetchInterval: time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, RPCDeadline: 20 * time.Millisecond, ReportRetries: 0, ReportRetryBase: time.Millisecond, GracePeriod: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, client
}
