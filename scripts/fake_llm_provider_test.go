package main

import (
	"context"
	"fmt"
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

func TestFakeProviderAgentToolRoundAndFinal(t *testing.T) {
	provider := &fakeProvider{apiKey: "secret", attempts: map[string]int{}}
	toolSchema := `{"type":"function","function":{"name":"%s","description":"test","parameters":{"type":"object"}}}`
	tools := fmt.Sprintf("["+toolSchema+","+toolSchema+","+toolSchema+"]", "search_code", "read_file", "read_docs")
	firstBody := `{"model":"model-a","messages":[{"role":"user","content":"agent-basic"}],"tools":` + tools + `,"tool_choice":"auto","max_completion_tokens":20,"stream":false}`
	first := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(firstBody))
	first.Header.Set("Authorization", "Bearer secret")
	firstResponse := httptest.NewRecorder()
	provider.complete(firstResponse, first)
	if firstResponse.Code != http.StatusOK || !strings.Contains(firstResponse.Body.String(), `"tool_calls"`) || !strings.Contains(firstResponse.Body.String(), `"read_file"`) {
		t.Fatalf("first status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	secondBody := `{"model":"model-a","messages":[{"role":"user","content":"agent-basic"},{"role":"assistant","tool_calls":[{"id":"call-readme","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"README.md\"}"}}]},{"role":"tool","tool_call_id":"call-readme","content":"{\"content\":\"README\"}"}],"tools":` + tools + `,"tool_choice":"auto","max_completion_tokens":20,"stream":false}`
	second := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(secondBody))
	second.Header.Set("Authorization", "Bearer secret")
	secondResponse := httptest.NewRecorder()
	provider.complete(secondResponse, second)
	if secondResponse.Code != http.StatusOK || !strings.Contains(secondResponse.Body.String(), `repository_diagnosis`) || !strings.Contains(secondResponse.Body.String(), `finish_reason`) {
		t.Fatalf("second status=%d body=%s", secondResponse.Code, secondResponse.Body.String())
	}
}
