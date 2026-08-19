package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestToolboxSearchReadAndDocs(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "main.go"), "package main\nfunc retryQueue() {}\n")
	mustWrite(t, filepath.Join(root, "docs", "runbook.md"), "# Runbook\nRetry queue overload\n")
	toolbox := newTestToolbox(t, root, ToolLimits{MaxFileBytes: 4096, MaxResultBytes: 4096, MaxMatches: 10})

	search, err := toolbox.Execute(context.Background(), "gateway", ToolSearchCode, json.RawMessage(`{"query":"retry"}`))
	if err != nil || !strings.Contains(string(search), `"path":"docs/runbook.md"`) || !strings.Contains(string(search), `"path":"main.go"`) {
		t.Fatalf("search=%s err=%v", search, err)
	}
	docs, err := toolbox.Execute(context.Background(), "gateway", ToolReadDocs, json.RawMessage(`{"query":"overload"}`))
	if err != nil || strings.Contains(string(docs), "main.go") || !strings.Contains(string(docs), "runbook.md") {
		t.Fatalf("docs=%s err=%v", docs, err)
	}
	read, err := toolbox.Execute(context.Background(), "gateway", ToolReadFile, json.RawMessage(`{"path":"main.go","start_line":2,"end_line":2}`))
	if err != nil || !strings.Contains(string(read), "retryQueue") {
		t.Fatalf("read=%s err=%v", read, err)
	}
}

func TestToolboxRejectsTraversalSymlinkSecretsBinaryLargeAndMalformedCalls(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	mustWrite(t, filepath.Join(root, "ok.go"), "package ok\n")
	mustWrite(t, filepath.Join(root, ".env"), "TOKEN=do-not-read\n")
	mustWrite(t, filepath.Join(root, "large.go"), strings.Repeat("x", 65))
	if err := os.WriteFile(filepath.Join(root, "binary.go"), []byte{0, 1, 2}, 0o600); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(outside, "secret.go"), "password\n")
	if err := os.Symlink(filepath.Join(outside, "secret.go"), filepath.Join(root, "escape.go")); err != nil {
		t.Fatal(err)
	}
	toolbox := newTestToolbox(t, root, ToolLimits{MaxFileBytes: 64, MaxResultBytes: 1024, MaxMatches: 10})
	tests := []struct {
		name string
		tool string
		args string
	}{
		{"absolute", ToolReadFile, `{"path":"/etc/passwd"}`},
		{"traversal", ToolReadFile, `{"path":"../secret.go"}`},
		{"passwd traversal", ToolReadFile, `{"path":"../../etc/passwd"}`},
		{"symlink", ToolReadFile, `{"path":"escape.go"}`},
		{"secret", ToolReadFile, `{"path":".env"}`},
		{"large", ToolReadFile, `{"path":"large.go"}`},
		{"binary", ToolReadFile, `{"path":"binary.go"}`},
		{"missing", ToolReadFile, `{"path":"missing.go"}`},
		{"unknown tool", "shell", `{}`},
		{"unknown arg", ToolReadFile, `{"path":"ok.go","command":"cat"}`},
		{"invalid type", ToolReadFile, `{"path":123}`},
		{"missing required", ToolReadFile, `{"start_line":1}`},
		{"malformed", ToolReadFile, `{"path":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if result, err := toolbox.Execute(context.Background(), "gateway", test.tool, json.RawMessage(test.args)); err == nil {
				t.Fatalf("expected rejection, got %s", result)
			}
		})
	}
}

func TestToolboxBoundsMatchesAndResultBytes(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "many.go"), "hit\nhit\nhit\n")
	toolbox := newTestToolbox(t, root, ToolLimits{MaxFileBytes: 1024, MaxResultBytes: 1024, MaxMatches: 2})
	result, err := toolbox.Execute(context.Background(), "gateway", ToolSearchCode, json.RawMessage(`{"query":"hit"}`))
	if err != nil || strings.Count(string(result), `"path"`) != 2 || !strings.Contains(string(result), `"truncated":true`) {
		t.Fatalf("result=%s err=%v", result, err)
	}
	tooSmall := newTestToolbox(t, root, ToolLimits{MaxFileBytes: 1024, MaxResultBytes: 10, MaxMatches: 2})
	if _, err := tooSmall.Execute(context.Background(), "gateway", ToolReadFile, json.RawMessage(`{"path":"many.go"}`)); err == nil {
		t.Fatal("expected result byte limit")
	}
}

func newTestToolbox(t *testing.T, root string, limits ToolLimits) *Toolbox {
	t.Helper()
	toolbox, err := NewToolbox(map[string]string{"gateway": root}, limits)
	if err != nil {
		t.Fatal(err)
	}
	return toolbox
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
