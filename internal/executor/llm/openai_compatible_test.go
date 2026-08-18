package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenAICompatibleSuccess(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Path != "/v1/chat/completions" {
			t.Errorf("path=%s", request.URL.Path)
		}
		if authorization := request.Header.Get("Authorization"); authorization != "Bearer test-secret" {
			t.Errorf("authorization=%q", authorization)
		}
		body, _ := io.ReadAll(request.Body)
		if !strings.Contains(string(body), `"response_format":{"type":"json_object"}`) {
			t.Errorf("request body=%s", body)
		}
		if !strings.Contains(string(body), `"max_completion_tokens":20`) || !strings.Contains(string(body), `"stream":false`) {
			t.Errorf("request body=%s", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"model":"model-a","choices":[{"message":{"content":"{\"ok\":true}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`)
	}))
	defer server.Close()
	provider := newTestProvider(t, server.URL, 1024)
	response, err := provider.Generate(context.Background(), Request{Model: "model-a", Messages: []Message{{Role: "user", Content: "hello"}}, MaxOutputTokens: 20, ResponseFormat: "json_object"})
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 || response.Content != `{"ok":true}` || response.Usage.TotalTokens != 15 || response.FinishReason != "stop" {
		t.Fatalf("unexpected response: %+v requests=%d", response, requests.Load())
	}
}

func TestOpenAICompatibleClassifiesStatusWithoutLeakingBodyOrKey(t *testing.T) {
	tests := []struct {
		status    int
		kind      ErrorKind
		retryable bool
	}{
		{http.StatusTooManyRequests, ErrorRateLimited, true},
		{http.StatusInternalServerError, ErrorUpstream, true},
		{http.StatusBadRequest, ErrorInvalidRequest, false},
		{http.StatusUnauthorized, ErrorAuthentication, false},
		{http.StatusForbidden, ErrorAuthentication, false},
		{http.StatusNotFound, ErrorModelNotFound, false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, `{"error":"secret-provider-detail"}`)
			}))
			defer server.Close()
			_, err := newTestProvider(t, server.URL, 1024).Generate(context.Background(), Request{Model: "model-a", Messages: []Message{{Role: "user", Content: "hello"}}, MaxOutputTokens: 20})
			providerError, ok := AsProviderError(err)
			if !ok || providerError.Kind != test.kind || providerError.Retryable != test.retryable || providerError.StatusCode != test.status {
				t.Fatalf("error=%+v", providerError)
			}
			if strings.Contains(err.Error(), "secret-provider-detail") || strings.Contains(err.Error(), "test-secret") {
				t.Fatalf("sensitive detail leaked: %v", err)
			}
		})
	}
}

func TestOpenAICompatibleInvalidAndOversizedResponses(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, `{`) }))
		defer server.Close()
		_, err := newTestProvider(t, server.URL, 1024).Generate(context.Background(), Request{})
		providerError, ok := AsProviderError(err)
		if !ok || providerError.Kind != ErrorInvalidResponse || !providerError.Retryable {
			t.Fatalf("error=%+v", providerError)
		}
	})
	t.Run("too large", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(writer, strings.Repeat("x", 33))
		}))
		defer server.Close()
		_, err := newTestProvider(t, server.URL, 32).Generate(context.Background(), Request{})
		providerError, ok := AsProviderError(err)
		if !ok || providerError.Kind != ErrorResponseTooLarge || providerError.Retryable {
			t.Fatalf("error=%+v", providerError)
		}
	})
}

func TestOpenAICompatibleHonorsCancelAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(250 * time.Millisecond) }))
	defer server.Close()
	provider := newTestProvider(t, server.URL, 1024)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := provider.Generate(canceled, Request{})
	providerError, ok := AsProviderError(err)
	if !ok || providerError.Kind != ErrorCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%+v", providerError)
	}

	timed, cancelTimeout := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelTimeout()
	_, err = provider.Generate(timed, Request{})
	providerError, ok = AsProviderError(err)
	if !ok || providerError.Kind != ErrorTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error=%+v", providerError)
	}
}

func TestOpenAICompatibleClassifiesClientTimeoutAndTransportFailure(t *testing.T) {
	t.Run("client timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(100 * time.Millisecond) }))
		defer server.Close()
		provider, err := NewOpenAICompatible(OpenAICompatibleConfig{BaseURL: server.URL, APIKey: "test-secret", RequestTimeout: 5 * time.Millisecond, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxResponseBytes: 1024, AllowHTTP: true})
		if err != nil {
			t.Fatal(err)
		}
		defer provider.CloseIdleConnections()
		_, err = provider.Generate(context.Background(), Request{})
		providerError, ok := AsProviderError(err)
		if !ok || providerError.Kind != ErrorTimeout || !providerError.Retryable {
			t.Fatalf("error=%+v", providerError)
		}
	})
	t.Run("connection failure", func(t *testing.T) {
		provider, err := NewOpenAICompatible(OpenAICompatibleConfig{BaseURL: "http://127.0.0.1:1", APIKey: "test-secret", RequestTimeout: time.Second, DialTimeout: 50 * time.Millisecond, TLSHandshakeTimeout: time.Second, MaxResponseBytes: 1024, AllowHTTP: true})
		if err != nil {
			t.Fatal(err)
		}
		defer provider.CloseIdleConnections()
		_, err = provider.Generate(context.Background(), Request{})
		providerError, ok := AsProviderError(err)
		if !ok || providerError.Kind != ErrorTransport || !providerError.Retryable {
			t.Fatalf("error=%+v", providerError)
		}
	})
}

func TestOpenAICompatibleRejectsInsecureURLByDefault(t *testing.T) {
	_, err := NewOpenAICompatible(OpenAICompatibleConfig{BaseURL: "http://provider.example", APIKey: "secret", RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxResponseBytes: 1024})
	if err == nil {
		t.Fatal("expected insecure URL error")
	}
}

func TestOpenAICompatibleDoesNotForwardKeyAcrossRedirects(t *testing.T) {
	var redirectedAuthorization string
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		redirectedAuthorization = request.Header.Get("Authorization")
		writer.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusFound)
	}))
	defer origin.Close()
	_, err := newTestProvider(t, origin.URL, 1024).Generate(context.Background(), Request{})
	providerError, ok := AsProviderError(err)
	if !ok || providerError.Kind != ErrorInvalidRequest || providerError.StatusCode != http.StatusFound {
		t.Fatalf("error=%+v", providerError)
	}
	if redirectedAuthorization != "" {
		t.Fatalf("authorization was forwarded to redirect target: %q", redirectedAuthorization)
	}
}

func newTestProvider(t *testing.T, baseURL string, maxResponseBytes int64) *OpenAICompatible {
	t.Helper()
	provider, err := NewOpenAICompatible(OpenAICompatibleConfig{BaseURL: baseURL, APIKey: "test-secret", RequestTimeout: time.Second, DialTimeout: time.Second, TLSHandshakeTimeout: time.Second, MaxResponseBytes: maxResponseBytes, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(provider.CloseIdleConnections)
	return provider
}
