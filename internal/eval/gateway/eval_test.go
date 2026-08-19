package gateway

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mrussss/orbit-scheduler/internal/executor/agent"
)

func TestGatewayFixturesAndDeterministicScoring(t *testing.T) {
	_, current, _, _ := runtime.Caller(0)
	fixtures := filepath.Join(filepath.Dir(current), "..", "..", "..", "evals", "gateway")
	cases, err := LoadCases(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range cases {
		result := agent.ResultContract{Diagnosis: agent.Diagnosis{ProblemType: item.Expected.ExpectedCategory, LikelyCauses: []string{"fixture cause"}, RecommendedChecks: []string{"fixture check"}, Limits: []string{}, Confidence: 0.8, CodeEvidence: []agent.Evidence{{Path: item.Expected.ExpectedFiles[0], Line: 1, Excerpt: joinEvidence(item.Expected.ExpectedEvidence)}}}, ModelCalls: 2, ToolCalls: 1, PromptTokens: 10, CompletionTokens: 5, EstimatedCostMicrounits: 2}
		metrics := Score(item, result)
		if !metrics.Success || !metrics.ExpectedFileHit || !metrics.ExpectedEvidenceHit || metrics.ForbiddenClaim || metrics.StepCount != 3 {
			t.Fatalf("%s metrics=%+v", item.ID, metrics)
		}
		result.Limits = append(result.Limits, item.Expected.ForbiddenClaims[0])
		if !Score(item, result).ForbiddenClaim {
			t.Fatalf("%s forbidden claim was not detected", item.ID)
		}
	}
}

func TestVerifyRepositoryRef(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".git", "refs", "heads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", "HEAD"), []byte("ref: refs/heads/main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", "refs", "heads", "main"), []byte("abc123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRepositoryRef(root, "abc123"); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRepositoryRef(root, "different"); err == nil {
		t.Fatal("expected ref mismatch")
	}
}

func joinEvidence(values []string) string {
	result := ""
	for _, value := range values {
		result += value + " "
	}
	return result
}
