package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/domain"
)

func TestBuildAgentEventsAndReplayCursor(t *testing.T) {
	now := time.Now().UTC()
	steps := []domain.AgentStep{{TaskID: uuid.New(), AttemptNo: 1, StepNo: 1, Kind: "TOOL", ToolName: "search_code", Status: "SUCCEEDED", StartedAt: now, FinishedAt: &now, InputSummary: []byte(`{"arguments_bytes":12}`), OutputSummary: []byte(`{"result_bytes":20}`)}}
	events := buildAgentEvents(steps)
	if len(events) != 4 || events[0].Name != "agent_step_started" || events[1].Name != "tool_call" || events[2].Name != "agent_step_finished" || events[3].Name != "tool_result" {
		t.Fatalf("events=%+v", events)
	}
	cursor, err := parseEventCursor(events[1].ID)
	if err != nil || compareEventCursor(events[2].cursor, cursor) <= 0 || compareEventCursor(events[0].cursor, cursor) >= 0 {
		t.Fatalf("cursor=%+v err=%v", cursor, err)
	}
	for _, invalid := range []string{"1", "a:1:0", "1:-1:0", "1:1:4"} {
		if _, err := parseEventCursor(invalid); err == nil {
			t.Fatalf("accepted cursor %q", invalid)
		}
	}
}
