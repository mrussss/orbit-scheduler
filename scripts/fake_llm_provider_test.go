package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFakeProviderSupportsExecutorRequestShape(t *testing.T) {
	provider := &fakeProvider{apiKey: "secret", attempts: map[string]int{}}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[{"role":"user","content":"hello"}],"temperature":0.2,"max_completion_tokens":20,"response_format":{"type":"json_object"},"stream":false}`))
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	provider.complete(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"total_tokens":12`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestFakeProviderSlowOnceObservesCancelThenSucceeds(t *testing.T) {
	provider := &fakeProvider{apiKey: "secret", attempts: map[string]int{}}
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[{"role":"user","content":"slow-once"}],"max_completion_tokens":20,"stream":false}`)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		provider.complete(response, request)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fake provider did not observe request cancellation")
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[{"role":"user","content":"slow-once"}],"max_completion_tokens":20,"stream":false}`))
	request.Header.Set("Authorization", "Bearer secret")
	response = httptest.NewRecorder()
	provider.complete(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"total_tokens":12`) {
		t.Fatalf("second status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestFakeProviderRateLimitsMatchingPromptOnce(t *testing.T) {
	provider := &fakeProvider{apiKey: "secret", attempts: map[string]int{}}
	for attempt, expected := range []int{http.StatusTooManyRequests, http.StatusOK} {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[{"role":"user","content":"retry-once"}],"max_completion_tokens":20,"stream":false}`))
		request.Header.Set("Authorization", "Bearer secret")
		response := httptest.NewRecorder()
		provider.complete(response, request)
		if response.Code != expected {
			t.Fatalf("attempt=%d status=%d want=%d", attempt+1, response.Code, expected)
		}
	}
}
