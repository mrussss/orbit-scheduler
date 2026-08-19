package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mrussss/orbit-scheduler/internal/executor/agent"
)

type Input struct {
	RepositoryRef  string `json:"repository_ref"`
	RepositoryRoot string `json:"repository_root"`
	Issue          string `json:"issue"`
	ErrorLog       string `json:"error_log"`
}

type Expected struct {
	ExpectedFiles    []string `json:"expected_files"`
	ExpectedEvidence []string `json:"expected_evidence"`
	ExpectedCategory string   `json:"expected_category"`
	ForbiddenClaims  []string `json:"forbidden_claims"`
}

type Case struct {
	ID       string
	Input    Input
	Expected Expected
	FaultLog string
}

type Metrics struct {
	CaseID                  string `json:"case_id"`
	Success                 bool   `json:"success"`
	ExpectedFileHit         bool   `json:"expected_file_hit"`
	ExpectedEvidenceHit     bool   `json:"expected_evidence_hit"`
	ForbiddenClaim          bool   `json:"forbidden_claim"`
	StepCount               int    `json:"step_count"`
	LatencyMS               int64  `json:"latency_ms"`
	PromptTokens            int    `json:"prompt_tokens"`
	CompletionTokens        int    `json:"completion_tokens"`
	EstimatedCostMicrounits int64  `json:"estimated_cost_microunits"`
}

func LoadCases(root string) ([]Case, error) {
	directories, err := filepath.Glob(filepath.Join(root, "case-*"))
	if err != nil {
		return nil, err
	}
	sort.Strings(directories)
	cases := make([]Case, 0, len(directories))
	for _, directory := range directories {
		info, err := os.Stat(directory)
		if err != nil || !info.IsDir() {
			continue
		}
		var item Case
		item.ID = filepath.Base(directory)
		if err := readStrict(filepath.Join(directory, "input.json"), &item.Input); err != nil {
			return nil, fmt.Errorf("%s input: %w", item.ID, err)
		}
		if err := readStrict(filepath.Join(directory, "expected.json"), &item.Expected); err != nil {
			return nil, fmt.Errorf("%s expected: %w", item.ID, err)
		}
		fault, err := os.ReadFile(filepath.Join(directory, "fault.log"))
		if err != nil {
			return nil, fmt.Errorf("%s fault log: %w", item.ID, err)
		}
		item.FaultLog = string(fault)
		if item.Input.RepositoryRef == "" || item.Input.RepositoryRoot == "" || item.Input.Issue == "" || len(item.Expected.ExpectedFiles) == 0 || len(item.Expected.ExpectedEvidence) == 0 || item.Expected.ExpectedCategory == "" {
			return nil, fmt.Errorf("%s is incomplete", item.ID)
		}
		cases = append(cases, item)
	}
	if len(cases) != 10 {
		return nil, fmt.Errorf("expected exactly 10 Gateway eval cases, got %d", len(cases))
	}
	return cases, nil
}

func VerifyRepositoryRef(root, expected string) error {
	gitPath := filepath.Join(root, ".git")
	info, err := os.Stat(gitPath)
	if err != nil {
		return fmt.Errorf("read repository metadata: %w", err)
	}
	gitDir := gitPath
	if !info.IsDir() {
		raw, err := os.ReadFile(gitPath)
		if err != nil {
			return err
		}
		line := strings.TrimSpace(string(raw))
		if !strings.HasPrefix(line, "gitdir: ") {
			return errors.New("invalid .git indirection")
		}
		gitDir = strings.TrimSpace(strings.TrimPrefix(line, "gitdir: "))
		if !filepath.IsAbs(gitDir) {
			gitDir = filepath.Join(root, gitDir)
		}
	}
	head, err := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if err != nil {
		return err
	}
	value := strings.TrimSpace(string(head))
	if strings.HasPrefix(value, "ref: ") {
		ref := strings.TrimSpace(strings.TrimPrefix(value, "ref: "))
		if raw, readErr := os.ReadFile(filepath.Join(gitDir, filepath.FromSlash(ref))); readErr == nil {
			value = strings.TrimSpace(string(raw))
		} else {
			packed, packedErr := os.ReadFile(filepath.Join(gitDir, "packed-refs"))
			if packedErr != nil {
				return fmt.Errorf("resolve repository HEAD: %w", readErr)
			}
			value = ""
			for _, line := range strings.Split(string(packed), "\n") {
				fields := strings.Fields(line)
				if len(fields) == 2 && fields[1] == ref {
					value = fields[0]
					break
				}
			}
		}
	}
	if value != expected {
		return fmt.Errorf("Gateway snapshot is %q, expected %q", value, expected)
	}
	return nil
}

func Score(item Case, result agent.ResultContract) Metrics {
	metrics := Metrics{CaseID: item.ID, StepCount: result.ModelCalls + result.ToolCalls, LatencyMS: result.LatencyMS, PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens, EstimatedCostMicrounits: result.EstimatedCostMicrounits}
	evidenceText := strings.Builder{}
	paths := make([]string, 0, len(result.CodeEvidence))
	for _, evidence := range result.CodeEvidence {
		paths = append(paths, strings.ToLower(filepath.ToSlash(evidence.Path)))
		evidenceText.WriteString(strings.ToLower(evidence.Path + " " + evidence.Excerpt + " "))
	}
	metrics.ExpectedFileHit = anyContains(paths, item.Expected.ExpectedFiles)
	metrics.ExpectedEvidenceHit = allContained(evidenceText.String(), item.Expected.ExpectedEvidence)
	encoded, _ := json.Marshal(result.Diagnosis)
	metrics.ForbiddenClaim = anyTextContained(strings.ToLower(string(encoded)), item.Expected.ForbiddenClaims)
	metrics.Success = strings.EqualFold(result.ProblemType, item.Expected.ExpectedCategory) && metrics.ExpectedFileHit && metrics.ExpectedEvidenceHit && !metrics.ForbiddenClaim
	return metrics
}

func readStrict(path string, target any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON")
		}
		return err
	}
	return nil
}

func anyContains(actual, expected []string) bool {
	for _, wanted := range expected {
		wanted = strings.ToLower(filepath.ToSlash(wanted))
		for _, value := range actual {
			if value == wanted || strings.HasSuffix(value, "/"+wanted) {
				return true
			}
		}
	}
	return false
}

func allContained(haystack string, needles []string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, strings.ToLower(needle)) {
			return false
		}
	}
	return true
}

func anyTextContained(haystack string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, strings.ToLower(needle)) {
			return true
		}
	}
	return false
}
