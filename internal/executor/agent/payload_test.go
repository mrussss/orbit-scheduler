package agent

import "testing"

func TestParsePayloadStrict(t *testing.T) {
	repositories := map[string]string{"gateway": "/snapshot"}
	payload, err := ParsePayload([]byte(`{"repository_root":"gateway","issue":"queue stalls","error_log":"timeout"}`), repositories, 1024)
	if err != nil || payload.RepositoryRoot != "gateway" {
		t.Fatalf("payload=%+v err=%v", payload, err)
	}
	for _, raw := range []string{
		`{"repository_root":"missing","issue":"x"}`,
		`{"repository_root":"gateway","issue":""}`,
		`{"repository_root":"gateway","issue":"x","model":"tenant-controlled"}`,
		`{"repository_root":"gateway","issue":"x"} {}`,
	} {
		if _, err := ParsePayload([]byte(raw), repositories, 1024); err == nil {
			t.Fatalf("expected strict rejection for %s", raw)
		}
	}
}

func TestParseDiagnosisStrict(t *testing.T) {
	raw := `{"problem_type":"concurrency","likely_causes":["lost wakeup"],"code_evidence":[{"path":"queue.go","line":7,"excerpt":"wait()"}],"recommended_checks":["run race test"],"confidence":0.8,"limits":[]}`
	if _, err := ParseDiagnosis(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseDiagnosis(raw[:len(raw)-1] + `,"extra":true}`); err == nil {
		t.Fatal("expected unknown final field rejection")
	}
}
