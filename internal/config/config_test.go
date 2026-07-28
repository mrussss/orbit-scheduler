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
