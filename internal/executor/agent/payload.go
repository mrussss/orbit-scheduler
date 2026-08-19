package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const defaultMaxIssueBytes = 64 << 10

type Payload struct {
	RepositoryRoot string `json:"repository_root"`
	Issue          string `json:"issue"`
	ErrorLog       string `json:"error_log,omitempty"`
}

type Evidence struct {
	Path    string `json:"path"`
	Line    int    `json:"line,omitempty"`
	Excerpt string `json:"excerpt"`
}

type Diagnosis struct {
	ProblemType       string     `json:"problem_type"`
	LikelyCauses      []string   `json:"likely_causes"`
	CodeEvidence      []Evidence `json:"code_evidence"`
	RecommendedChecks []string   `json:"recommended_checks"`
	Confidence        float64    `json:"confidence"`
	Limits            []string   `json:"limits"`
}

type ResultContract struct {
	Diagnosis
	Model                   string `json:"model"`
	PromptTokens            int    `json:"prompt_tokens"`
	CompletionTokens        int    `json:"completion_tokens"`
	TotalTokens             int    `json:"total_tokens"`
	EstimatedCostMicrounits int64  `json:"estimated_cost_microunits"`
	ModelCalls              int    `json:"model_calls"`
	ToolCalls               int    `json:"tool_calls"`
	LatencyMS               int64  `json:"latency_ms"`
}

func ParsePayload(raw []byte, allowedRepositories map[string]string, maxIssueBytes int) (Payload, error) {
	var payload Payload
	if err := decodeStrict(raw, &payload); err != nil {
		return Payload{}, fmt.Errorf("decode agent payload: %w", err)
	}
	if maxIssueBytes <= 0 {
		maxIssueBytes = defaultMaxIssueBytes
	}
	if payload.RepositoryRoot == "" || allowedRepositories[payload.RepositoryRoot] == "" {
		return Payload{}, errors.New("repository_root is not allowlisted")
	}
	if strings.TrimSpace(payload.Issue) == "" || !utf8.ValidString(payload.Issue) || len(payload.Issue) > maxIssueBytes {
		return Payload{}, errors.New("issue is empty, invalid, or too large")
	}
	if !utf8.ValidString(payload.ErrorLog) || len(payload.ErrorLog) > maxIssueBytes {
		return Payload{}, errors.New("error_log is invalid or too large")
	}
	return payload, nil
}

func ParseDiagnosis(raw string) (Diagnosis, error) {
	var diagnosis Diagnosis
	if err := decodeStrict([]byte(raw), &diagnosis); err != nil {
		return Diagnosis{}, fmt.Errorf("decode final diagnosis: %w", err)
	}
	if strings.TrimSpace(diagnosis.ProblemType) == "" || len(diagnosis.LikelyCauses) == 0 || len(diagnosis.RecommendedChecks) == 0 || diagnosis.CodeEvidence == nil || diagnosis.Limits == nil || diagnosis.Confidence < 0 || diagnosis.Confidence > 1 {
		return Diagnosis{}, errors.New("final diagnosis does not satisfy the required schema")
	}
	if len(diagnosis.LikelyCauses) > 16 || len(diagnosis.CodeEvidence) > 32 || len(diagnosis.RecommendedChecks) > 32 || len(diagnosis.Limits) > 16 {
		return Diagnosis{}, errors.New("final diagnosis contains too many entries")
	}
	for _, evidence := range diagnosis.CodeEvidence {
		if evidence.Path == "" || evidence.Excerpt == "" || evidence.Line < 0 {
			return Diagnosis{}, errors.New("final diagnosis contains invalid evidence")
		}
	}
	return diagnosis, nil
}

func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}
