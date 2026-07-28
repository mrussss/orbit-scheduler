package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/business"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/gormrepo"
	"github.com/mrussss/orbit-scheduler/internal/middleware"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

const (
	maxPageSize       = 100
	maxJobTasks       = 500
	maxIdempotencyKey = 200
)

type handlers struct {
	service *business.Service
	cursors *CursorCodec
}

func newHandlers(service *business.Service, cursorSecret string) *handlers {
	return &handlers{service: service, cursors: NewCursorCodec(cursorSecret)}
}

type createProjectRequest struct {
	Name               string `json:"name" binding:"required"`
	TaskQuota          int64  `json:"task_quota"`
	MaxConcurrentTasks int    `json:"max_concurrent_tasks" binding:"required"`
}

func (h *handlers) createProject(c *gin.Context) {
	var req createProjectRequest
	if !bind(c, &req) {
		return
	}
	project, err := h.service.CreateProject(c.Request.Context(), business.CreateProjectInput{Name: req.Name, TaskQuota: req.TaskQuota, MaxConcurrentTasks: req.MaxConcurrentTasks})
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.JSON(http.StatusCreated, projectView(project))
}
func (h *handlers) listProjects(c *gin.Context) {
	projects, err := h.service.ListProjects(c.Request.Context())
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]any, len(projects))
	for i, p := range projects {
		items[i] = projectView(p)
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}
func (h *handlers) getProject(c *gin.Context) {
	id, ok := pathID(c, "project_id")
	if !ok {
		return
	}
	project, err := h.service.GetProject(c.Request.Context(), id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, projectView(project))
}

type updateProjectRequest struct {
	Name               *string               `json:"name"`
	Status             *domain.ProjectStatus `json:"status"`
	TaskQuota          *int64                `json:"task_quota"`
	MaxConcurrentTasks *int                  `json:"max_concurrent_tasks"`
}

func (h *handlers) updateProject(c *gin.Context) {
	id, ok := pathID(c, "project_id")
	if !ok {
		return
	}
	var req updateProjectRequest
	if !bind(c, &req) {
		return
	}
	project, err := h.service.UpdateProject(c.Request.Context(), id, req.Name, req.Status, req.TaskQuota, req.MaxConcurrentTasks)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, projectView(project))
}
func projectView(p domain.Project) gin.H {
	return gin.H{"id": p.ID, "name": p.Name, "status": p.Status, "task_quota": p.TaskQuota, "max_concurrent_tasks": p.MaxConcurrentTasks, "created_at": p.CreatedAt, "updated_at": p.UpdatedAt}
}

type createTokenRequest struct {
	Name      string     `json:"name" binding:"required"`
	Scopes    []string   `json:"scopes" binding:"required"`
	ExpiresAt *time.Time `json:"expires_at"`
}

func (h *handlers) createToken(c *gin.Context) {
	projectID, ok := pathID(c, "project_id")
	if !ok {
		return
	}
	var req createTokenRequest
	if !bind(c, &req) {
		return
	}
	created, err := h.service.CreateToken(c.Request.Context(), projectID, req.Name, req.Scopes, req.ExpiresAt)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	view := tokenView(created.Token)
	view["token"] = created.Plaintext
	c.JSON(http.StatusCreated, view)
}
func (h *handlers) listTokens(c *gin.Context) {
	projectID, ok := pathID(c, "project_id")
	if !ok {
		return
	}
	tokens, err := h.service.ListTokens(c.Request.Context(), projectID)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]any, len(tokens))
	for i, t := range tokens {
		items[i] = tokenView(t)
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}
func (h *handlers) disableToken(c *gin.Context) {
	projectID, ok := pathID(c, "project_id")
	if !ok {
		return
	}
	tokenID, ok := pathID(c, "token_id")
	if !ok {
		return
	}
	if err := h.service.DisableToken(c.Request.Context(), projectID, tokenID); err != nil {
		writeServiceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func tokenView(t domain.APIToken) gin.H {
	return gin.H{"id": t.ID, "project_id": t.ProjectID, "prefix": t.TokenPrefix, "name": t.Name, "scopes": t.Scopes, "disabled": t.Disabled, "expires_at": t.ExpiresAt, "last_used_at": t.LastUsedAt, "created_at": t.CreatedAt}
}

type createTaskRequest struct {
	JobID              *uuid.UUID      `json:"job_id"`
	TaskType           string          `json:"task_type" binding:"required"`
	Payload            json.RawMessage `json:"payload" binding:"required"`
	Priority           int             `json:"priority"`
	AvailableAt        *time.Time      `json:"available_at"`
	ExecutionTimeoutMS int64           `json:"execution_timeout_ms" binding:"required"`
	OverallDeadline    *time.Time      `json:"overall_deadline"`
	MaxAttempts        int             `json:"max_attempts" binding:"required"`
}

func (r createTaskRequest) command() command.CreateTask {
	available := time.Now().UTC()
	if r.AvailableAt != nil {
		available = r.AvailableAt.UTC()
	}
	return command.CreateTask{JobID: r.JobID, TaskType: r.TaskType, Payload: r.Payload, Priority: r.Priority, AvailableAt: available, ExecutionTimeout: time.Duration(r.ExecutionTimeoutMS) * time.Millisecond, OverallDeadline: r.OverallDeadline, MaxAttempts: r.MaxAttempts}
}
func (h *handlers) createTask(c *gin.Context) {
	var req createTaskRequest
	if !bind(c, &req) {
		return
	}
	input := req.command()
	input.IdempotencyKey = strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	input.TraceID = c.GetString("trace_id")
	if len(input.IdempotencyKey) > maxIdempotencyKey {
		WriteError(c, 400, "INVALID_IDEMPOTENCY_KEY", "idempotency key is too long", nil)
		return
	}
	created, err := h.service.CreateTask(c.Request.Context(), middleware.Principal(c), input)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	status := http.StatusCreated
	if !created.Created {
		status = http.StatusOK
	}
	c.JSON(status, taskView(created.Task, true))
}
func (h *handlers) listTasks(c *gin.Context) {
	filter, limit, ok := h.taskFilter(c)
	if !ok {
		return
	}
	tasks, err := h.service.ListTasks(c.Request.Context(), middleware.Principal(c), filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]any, len(tasks))
	for i, t := range tasks {
		items[i] = taskView(t, false)
	}
	var next string
	if len(tasks) == limit {
		last := tasks[len(tasks)-1]
		next, _ = h.cursors.Encode(Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	c.JSON(200, gin.H{"items": items, "next_cursor": next})
}
func (h *handlers) getTask(c *gin.Context) {
	id, ok := pathID(c, "task_id")
	if !ok {
		return
	}
	task, err := h.service.GetTask(c.Request.Context(), middleware.Principal(c), id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.JSON(200, taskView(task, true))
}
func (h *handlers) cancelTask(c *gin.Context) {
	id, ok := pathID(c, "task_id")
	if !ok {
		return
	}
	if err := h.service.CancelTask(c.Request.Context(), middleware.Principal(c), id); err != nil {
		writeServiceError(c, err)
		return
	}
	c.Status(http.StatusAccepted)
}
func (h *handlers) listAttempts(c *gin.Context) {
	id, ok := pathID(c, "task_id")
	if !ok {
		return
	}
	attempts, err := h.service.ListAttempts(c.Request.Context(), middleware.Principal(c), id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]any, len(attempts))
	for i, a := range attempts {
		items[i] = gin.H{"task_id": a.TaskID, "attempt_no": a.AttemptNo, "worker_name": a.WorkerName, "worker_instance_id": a.WorkerInstanceID, "started_at": a.StartedAt, "finished_at": a.FinishedAt, "outcome": a.Outcome, "error_type": a.ErrorType, "error_message": a.ErrorMessage, "execution_duration_ms": a.ExecutionDurationMS, "lease_expired": a.LeaseExpired}
	}
	c.JSON(200, gin.H{"items": items})
}
func (h *handlers) getResult(c *gin.Context) {
	id, ok := pathID(c, "task_id")
	if !ok {
		return
	}
	task, err := h.service.GetTask(c.Request.Context(), middleware.Principal(c), id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	if !task.Status.Terminal() {
		WriteError(c, http.StatusConflict, "RESULT_NOT_READY", "task has not reached a terminal state", nil)
		return
	}
	c.JSON(200, gin.H{"task_id": task.ID, "status": task.Status, "result": json.RawMessage(task.Result), "error_type": task.FinalErrorType, "error_message": task.FinalErrorMessage, "completed_at": task.CompletedAt})
}
func taskView(t domain.Task, detail bool) gin.H {
	view := gin.H{"id": t.ID, "project_id": t.ProjectID, "job_id": t.JobID, "task_type": t.TaskType, "status": t.Status, "priority": t.Priority, "available_at": t.AvailableAt, "execution_timeout_ms": t.ExecutionTimeout.Milliseconds(), "overall_deadline": t.OverallDeadline, "max_attempts": t.MaxAttempts, "attempt_no": t.AttemptNo, "cancel_requested_at": t.CancelRequestedAt, "created_at": t.CreatedAt, "updated_at": t.UpdatedAt, "completed_at": t.CompletedAt}
	if detail {
		view["payload"] = json.RawMessage(t.Payload)
	}
	return view
}
func (h *handlers) taskFilter(c *gin.Context) (gormrepo.TaskFilter, int, bool) {
	limit, ok := pageSize(c)
	if !ok {
		return gormrepo.TaskFilter{}, 0, false
	}
	filter := gormrepo.TaskFilter{Status: c.Query("status"), TaskType: c.Query("task_type"), Limit: limit}
	if filter.Status != "" && !domain.TaskStatus(filter.Status).Valid() {
		WriteError(c, 400, "INVALID_STATUS", "invalid task status", nil)
		return filter, 0, false
	}
	if raw := c.Query("job_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			WriteError(c, 400, "INVALID_JOB_ID", "job_id must be a UUID", nil)
			return filter, 0, false
		}
		filter.JobID = &id
	}
	if raw := c.Query("priority"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			WriteError(c, 400, "INVALID_PRIORITY", "priority must be an integer", nil)
			return filter, 0, false
		}
		filter.Priority = &value
	}
	for key, target := range map[string]**time.Time{"created_after": &filter.CreatedAfter, "created_before": &filter.CreatedBefore, "available_before": &filter.AvailableBefore} {
		if raw := c.Query(key); raw != "" {
			value, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				WriteError(c, 400, "INVALID_TIME", key+" must be RFC3339", nil)
				return filter, 0, false
			}
			utc := value.UTC()
			*target = &utc
		}
	}
	if raw := c.Query("cursor"); raw != "" {
		cursor, err := h.cursors.Decode(raw)
		if err != nil {
			WriteError(c, 400, "INVALID_CURSOR", err.Error(), nil)
			return filter, 0, false
		}
		filter.CursorCreatedAt = &cursor.CreatedAt
		filter.CursorID = &cursor.ID
	}
	return filter, limit, true
}

type createJobRequest struct {
	Name     string              `json:"name" binding:"required"`
	Metadata json.RawMessage     `json:"metadata"`
	Tasks    []createTaskRequest `json:"tasks" binding:"required"`
}

func (h *handlers) createJob(c *gin.Context) {
	var req createJobRequest
	if !bind(c, &req) {
		return
	}
	if len(req.Tasks) == 0 || len(req.Tasks) > maxJobTasks {
		WriteError(c, 400, "INVALID_BATCH_SIZE", "job task count is outside the allowed range", map[string]any{"max": maxJobTasks})
		return
	}
	if len(req.Metadata) == 0 {
		req.Metadata = []byte(`{}`)
	}
	input := command.CreateJob{Name: req.Name, Metadata: req.Metadata, IdempotencyKey: strings.TrimSpace(c.GetHeader("Idempotency-Key")), TraceID: c.GetString("trace_id"), Tasks: make([]command.CreateTask, len(req.Tasks))}
	for i, t := range req.Tasks {
		input.Tasks[i] = t.command()
	}
	created, err := h.service.CreateJob(c.Request.Context(), middleware.Principal(c), input)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	status := 201
	if !created.Created {
		status = 200
	}
	c.JSON(status, gin.H{"job": jobView(created.Job, domain.JobCounts{}), "tasks": taskViews(created.Tasks, false)})
}
func (h *handlers) listJobs(c *gin.Context) {
	limit, ok := pageSize(c)
	if !ok {
		return
	}
	filter := gormrepo.JobFilter{Limit: limit}
	if raw := c.Query("cursor"); raw != "" {
		cursor, err := h.cursors.Decode(raw)
		if err != nil {
			WriteError(c, 400, "INVALID_CURSOR", err.Error(), nil)
			return
		}
		filter.CursorCreatedAt = &cursor.CreatedAt
		filter.CursorID = &cursor.ID
	}
	jobs, counts, err := h.service.ListJobs(c.Request.Context(), middleware.Principal(c), filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	items := make([]any, len(jobs))
	for i, j := range jobs {
		items[i] = jobView(j, counts[i])
	}
	var next string
	if len(jobs) == limit {
		last := jobs[len(jobs)-1]
		next, _ = h.cursors.Encode(Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	c.JSON(200, gin.H{"items": items, "next_cursor": next})
}
func (h *handlers) getJob(c *gin.Context) {
	id, ok := pathID(c, "job_id")
	if !ok {
		return
	}
	job, counts, err := h.service.GetJob(c.Request.Context(), middleware.Principal(c), id)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.JSON(200, jobView(job, counts))
}
func (h *handlers) cancelJob(c *gin.Context) {
	id, ok := pathID(c, "job_id")
	if !ok {
		return
	}
	if err := h.service.CancelJob(c.Request.Context(), middleware.Principal(c), id); err != nil {
		writeServiceError(c, err)
		return
	}
	c.Status(202)
}
func (h *handlers) listJobTasks(c *gin.Context) {
	id, ok := pathID(c, "job_id")
	if !ok {
		return
	}
	filter, limit, ok := h.taskFilter(c)
	if !ok {
		return
	}
	filter.JobID = &id
	tasks, err := h.service.ListTasks(c.Request.Context(), middleware.Principal(c), filter)
	if err != nil {
		writeServiceError(c, err)
		return
	}
	c.JSON(200, gin.H{"items": taskViews(tasks, false), "page_size": limit})
}
func jobView(j domain.Job, c domain.JobCounts) gin.H {
	return gin.H{"id": j.ID, "project_id": j.ProjectID, "name": j.Name, "metadata": json.RawMessage(j.Metadata), "cancel_requested_at": j.CancelRequestedAt, "created_at": j.CreatedAt, "updated_at": j.UpdatedAt, "counts": c, "derived_status": domain.DeriveJobStatus(c)}
}
func taskViews(tasks []domain.Task, detail bool) []any {
	items := make([]any, len(tasks))
	for i, t := range tasks {
		items[i] = taskView(t, detail)
	}
	return items
}

func bind(c *gin.Context, out any) bool {
	if err := c.ShouldBindJSON(out); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			WriteError(c, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "request body exceeds the configured limit", nil)
		} else {
			WriteError(c, http.StatusBadRequest, "INVALID_JSON", "request body is invalid", map[string]any{"reason": err.Error()})
		}
		return false
	}
	return true
}
func pathID(c *gin.Context, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param(name))
	if err != nil {
		WriteError(c, 400, "INVALID_ID", name+" must be a UUID", nil)
		return uuid.Nil, false
	}
	return id, true
}
func pageSize(c *gin.Context) (int, bool) {
	raw := c.DefaultQuery("page_size", "50")
	size, err := strconv.Atoi(raw)
	if err != nil || size < 1 || size > maxPageSize {
		WriteError(c, 400, "INVALID_PAGE_SIZE", "page_size must be between 1 and 100", nil)
		return 0, false
	}
	return size, true
}
func writeServiceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		WriteError(c, 504, "REQUEST_TIMEOUT", "request deadline exceeded", nil)
	case errors.Is(err, scheduler.ErrNotFound):
		WriteError(c, 404, "NOT_FOUND", "resource does not exist", nil)
	case errors.Is(err, command.ErrIdempotencyConflict), errors.Is(err, scheduler.ErrConflict):
		WriteError(c, 409, "CONFLICT", err.Error(), nil)
	case errors.Is(err, command.ErrProjectDisabled):
		WriteError(c, 409, "PROJECT_DISABLED", err.Error(), nil)
	case errors.Is(err, command.ErrQuotaExceeded):
		WriteError(c, 409, "QUOTA_EXCEEDED", err.Error(), nil)
	case errors.Is(err, command.ErrInvalidCreate), errors.Is(err, business.ErrInvalidArgument):
		WriteError(c, 400, "INVALID_ARGUMENT", err.Error(), nil)
	default:
		WriteError(c, 500, "INTERNAL", "internal server error", nil)
	}
}
