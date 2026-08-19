package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/middleware"
)

type eventCursor struct{ Attempt, Step, Phase int }

type agentEvent struct {
	ID, Name string
	Data     any
	cursor   eventCursor
}

func (h *handlers) taskEvents(c *gin.Context) {
	taskID, ok := pathID(c, "task_id")
	if !ok {
		return
	}
	cursor, err := parseEventCursor(c.GetHeader("Last-Event-ID"))
	if err != nil {
		WriteError(c, http.StatusBadRequest, "INVALID_EVENT_CURSOR", "Last-Event-ID is invalid", nil)
		return
	}
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		WriteError(c, http.StatusInternalServerError, "STREAM_UNAVAILABLE", "streaming is unavailable", nil)
		return
	}
	// The regular API server has a finite WriteTimeout. Disable it only for this
	// authenticated stream; request cancellation still terminates the handler.
	_ = http.NewResponseController(c.Writer).SetWriteDeadline(time.Time{})
	principal := middleware.Principal(c)
	task, steps, err := h.service.ListAgentSteps(c.Request.Context(), principal, taskID)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache, no-transform")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	lastStatus := domain.TaskStatus("")
	ticker := time.NewTicker(250 * time.Millisecond)
	heartbeat := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer heartbeat.Stop()
	for {
		if task.Status != lastStatus {
			if err := writeSSE(c, "", "task_status", map[string]any{"task_id": task.ID, "attempt_no": task.AttemptNo, "status": task.Status, "updated_at": task.UpdatedAt.UTC()}); err != nil {
				return
			}
			lastStatus = task.Status
		}
		for _, event := range buildAgentEvents(steps) {
			if compareEventCursor(event.cursor, cursor) <= 0 {
				continue
			}
			if err := writeSSE(c, event.ID, event.Name, event.Data); err != nil {
				return
			}
			cursor = event.cursor
		}
		flusher.Flush()
		if task.Status.Terminal() {
			if task.Status == domain.TaskSucceeded && len(task.Result) > 0 {
				if err := writeSSE(c, "", "final_result", map[string]any{"task_id": task.ID, "attempt_no": task.AttemptNo, "status": task.Status, "result": json.RawMessage(task.Result), "completed_at": task.CompletedAt}); err != nil {
					return
				}
				flusher.Flush()
			} else if task.Status == domain.TaskFailed || task.Status == domain.TaskCanceled {
				if err := writeSSE(c, "", "error", map[string]any{"task_id": task.ID, "attempt_no": task.AttemptNo, "status": task.Status, "error_type": task.FinalErrorType, "error_message": task.FinalErrorMessage, "completed_at": task.CompletedAt}); err != nil {
					return
				}
				flusher.Flush()
			}
			return
		}
		select {
		case <-c.Request.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(c.Writer, ": keepalive\n\n")
			flusher.Flush()
		case <-ticker.C:
			task, steps, err = h.service.ListAgentSteps(c.Request.Context(), principal, taskID)
			if err != nil {
				_ = writeSSE(c, "", "error", map[string]any{"code": "TRACE_READ_FAILED"})
				flusher.Flush()
				return
			}
		}
	}
}

func buildAgentEvents(steps []domain.AgentStep) []agentEvent {
	events := make([]agentEvent, 0, len(steps)*4)
	for _, step := range steps {
		base := map[string]any{"task_id": step.TaskID, "attempt_no": step.AttemptNo, "step_no": step.StepNo, "kind": step.Kind, "tool_name": step.ToolName, "started_at": step.StartedAt.UTC(), "input_summary": json.RawMessage(step.InputSummary)}
		events = append(events, newAgentEvent(step, 0, "agent_step_started", base))
		if step.Kind == "TOOL" {
			events = append(events, newAgentEvent(step, 1, "tool_call", base))
		}
		if step.FinishedAt == nil {
			continue
		}
		finished := map[string]any{"task_id": step.TaskID, "attempt_no": step.AttemptNo, "step_no": step.StepNo, "kind": step.Kind, "tool_name": step.ToolName, "status": step.Status, "started_at": step.StartedAt.UTC(), "finished_at": step.FinishedAt.UTC(), "output_summary": json.RawMessage(step.OutputSummary)}
		events = append(events, newAgentEvent(step, 2, "agent_step_finished", finished))
		special := ""
		switch step.Kind {
		case "TOOL":
			special = "tool_result"
		case "ERROR":
			special = "error"
		}
		if special != "" {
			events = append(events, newAgentEvent(step, 3, special, finished))
		}
	}
	return events
}

func newAgentEvent(step domain.AgentStep, phase int, name string, data any) agentEvent {
	cursor := eventCursor{Attempt: step.AttemptNo, Step: step.StepNo, Phase: phase}
	return agentEvent{ID: fmt.Sprintf("%d:%d:%d", cursor.Attempt, cursor.Step, cursor.Phase), Name: name, Data: data, cursor: cursor}
}

func parseEventCursor(raw string) (eventCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return eventCursor{}, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return eventCursor{}, fmt.Errorf("invalid event cursor")
	}
	values := make([]int, 3)
	for index, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return eventCursor{}, fmt.Errorf("invalid event cursor")
		}
		values[index] = value
	}
	if values[2] > 3 {
		return eventCursor{}, fmt.Errorf("invalid event cursor")
	}
	return eventCursor{Attempt: values[0], Step: values[1], Phase: values[2]}, nil
}

func compareEventCursor(a, b eventCursor) int {
	if a.Attempt != b.Attempt {
		return a.Attempt - b.Attempt
	}
	if a.Step != b.Step {
		return a.Step - b.Step
	}
	return a.Phase - b.Phase
}

func writeSSE(c *gin.Context, id, name string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if id != "" {
		if _, err := fmt.Fprintf(c.Writer, "id: %s\n", id); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", name, raw)
	return err
}
