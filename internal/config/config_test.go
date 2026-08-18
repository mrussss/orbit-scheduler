package config

import (
	"strings"
	"testing"
)

func TestLoadValidatesRequiredSecrets(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TOKEN_PEPPER", "")
	t.Setenv("ADMIN_TOKEN", "")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") || !strings.Contains(err.Error(), "TOKEN_PEPPER") {
		t.Fatalf("expected joined validation error, got %v", err)
	}
}

func TestLoadValid(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/orbit")
	t.Setenv("TOKEN_PEPPER", strings.Repeat("x", 32))
	t.Setenv("ADMIN_TOKEN", strings.Repeat("a", 32))
	t.Setenv("WORKER_CAPACITY", "7")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.Capacity != 7 {
		t.Fatalf("capacity = %d", cfg.Worker.Capacity)
	}
}

func TestLoadWorkerValidatesLLMOnlyWhenEnabled(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	t.Setenv("WORKER_TASK_TYPES", "mock")
	t.Setenv("LLM_BASE_URL", "")
	t.Setenv("LLM_API_KEY", "")
	t.Setenv("LLM_ALLOWED_MODELS", "")
	if _, err := LoadWorker(); err != nil {
		t.Fatalf("LLM configuration should be optional without llm task type: %v", err)
	}

	t.Setenv("WORKER_TASK_TYPES", "mock,llm")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "LLM_BASE_URL") || !strings.Contains(err.Error(), "LLM_API_KEY") || !strings.Contains(err.Error(), "LLM_ALLOWED_MODELS") {
		t.Fatalf("expected joined LLM validation errors, got %v", err)
	}
}

func TestLoadWorkerLLMConfiguration(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	t.Setenv("WORKER_TASK_TYPES", "llm")
	t.Setenv("LLM_BASE_URL", "http://127.0.0.1:8089/v1")
	t.Setenv("LLM_API_KEY", "test-secret")
	t.Setenv("LLM_ALLOWED_MODELS", "model-a,model-b")
	t.Setenv("LLM_MAX_CONCURRENCY", "2")
	t.Setenv("LLM_COST_TABLE_JSON", `{"model-a":{"prompt_microunits_per_million_tokens":10,"completion_microunits_per_million_tokens":20}}`)
	cfg, err := LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LLMExecutor.APIKey != "test-secret" || cfg.LLMExecutor.MaxConcurrency != 2 || len(cfg.LLMExecutor.AllowedModels) != 2 {
		t.Fatalf("unexpected LLM config: %+v", cfg.LLMExecutor)
	}
	if cfg.LLMExecutor.CostTable["model-a"].CompletionMicrounitsPerMillionTokens != 20 {
		t.Fatalf("unexpected cost table: %+v", cfg.LLMExecutor.CostTable)
	}
}

func TestLoadWorkerRejectsInsecureLLMURLOutsideTest(t *testing.T) {
	for _, appEnv := range []string{"development", "production"} {
		t.Run(appEnv, func(t *testing.T) {
			t.Setenv("APP_ENV", appEnv)
			t.Setenv("WORKER_TASK_TYPES", "llm")
			t.Setenv("LLM_BASE_URL", "http://provider.example/v1")
			t.Setenv("LLM_API_KEY", "test-secret")
			t.Setenv("LLM_ALLOWED_MODELS", "model-a")
			_, err := LoadWorker()
			if err == nil || !strings.Contains(err.Error(), "HTTPS") {
				t.Fatalf("expected HTTPS validation error, got %v", err)
			}
		})
	}
}

func TestLoadWorkerRejectsUnimplementedLLMContentLoggingAndTools(t *testing.T) {
	t.Setenv("APP_ENV", "test")
	t.Setenv("WORKER_TASK_TYPES", "llm")
	t.Setenv("LLM_BASE_URL", "http://127.0.0.1:8089/v1")
	t.Setenv("LLM_API_KEY", "test-secret")
	t.Setenv("LLM_ALLOWED_MODELS", "model-a")
	t.Setenv("LLM_LOG_CONTENT", "true")
	t.Setenv("LLM_TOOL_CALLING_ENABLED", "true")
	_, err := LoadWorker()
	if err == nil || !strings.Contains(err.Error(), "LLM_LOG_CONTENT") || !strings.Contains(err.Error(), "tool calling") {
		t.Fatalf("expected secure baseline errors, got %v", err)
	}
}

func TestLoadWorkerRejectsUnknownLLMCostFields(t *testing.T) {
	t.Setenv("WORKER_TASK_TYPES", "mock")
	t.Setenv("LLM_COST_TABLE_JSON", `{"model-a":{"prompt_price":10}}`)
	_, err := LoadWorker()
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected strict cost table error, got %v", err)
	}
}
