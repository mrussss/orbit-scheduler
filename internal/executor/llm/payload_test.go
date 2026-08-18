package llm

import (
	"strings"
	"testing"
)

func TestParsePayloadValidatesContract(t *testing.T) {
	temperature := 0.2
	policy := PayloadPolicy{AllowedModels: map[string]struct{}{"model-a": {}}, MaxPromptBytes: 1024, MaxOutputTokens: 1000}
	payload, err := ParsePayload([]byte(`{"model":"model-a","messages":[{"role":"system","content":"Be concise"},{"role":"user","content":"hello"}],"temperature":0.2,"max_output_tokens":200,"response_format":"json_object","metadata":{"prompt_version":"v1"}}`), policy)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Model != "model-a" || payload.Temperature == nil || *payload.Temperature != temperature || payload.MaxOutputTokens != 200 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestParsePayloadRejectsSecretAndRoutingFields(t *testing.T) {
	policy := PayloadPolicy{AllowedModels: map[string]struct{}{"model-a": {}}, MaxPromptBytes: 1024, MaxOutputTokens: 1000}
	for _, field := range []string{`"api_key":"secret"`, `"base_url":"https://evil.example"`, `"headers":{"Authorization":"secret"}`} {
		raw := `{"model":"model-a","messages":[{"role":"user","content":"hello"}],"max_output_tokens":20,` + field + `}`
		if _, err := ParsePayload([]byte(raw), policy); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("field %s should be rejected, got %v", field, err)
		}
	}
}

func TestParsePayloadLimits(t *testing.T) {
	temperature := 2.1
	tests := []struct {
		name    string
		payload Payload
	}{
		{name: "model", payload: Payload{Model: "other", Messages: []Message{{Role: "user", Content: "hello"}}, MaxOutputTokens: 10}},
		{name: "role", payload: Payload{Model: "model-a", Messages: []Message{{Role: "tool", Content: "hello"}}, MaxOutputTokens: 10}},
		{name: "prompt", payload: Payload{Model: "model-a", Messages: []Message{{Role: "user", Content: "too-large"}}, MaxOutputTokens: 10}},
		{name: "temperature", payload: Payload{Model: "model-a", Messages: []Message{{Role: "user", Content: "hello"}}, Temperature: &temperature, MaxOutputTokens: 10}},
		{name: "tokens", payload: Payload{Model: "model-a", Messages: []Message{{Role: "user", Content: "hello"}}, MaxOutputTokens: 101}},
		{name: "response format", payload: Payload{Model: "model-a", Messages: []Message{{Role: "user", Content: "hello"}}, MaxOutputTokens: 10, ResponseFormat: "text"}},
	}
	policy := PayloadPolicy{AllowedModels: map[string]struct{}{"model-a": {}}, MaxPromptBytes: 8, MaxOutputTokens: 100}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.payload.Validate(policy); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
