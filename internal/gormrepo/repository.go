package gormrepo

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"gorm.io/gorm"
)

type Repository struct{ db *gorm.DB }

func New(db *gorm.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("gorm database is required")
	}
	return &Repository{db: db}, nil
}

type projectModel struct {
	ID                   uuid.UUID `gorm:"type:uuid;primaryKey"`
	Name                 string
	Status               domain.ProjectStatus
	TaskQuota            int64
	MaxConcurrentTasks   int
	CreatedAt, UpdatedAt time.Time
}

func (projectModel) TableName() string { return "projects" }

type tokenModel struct {
	ID                    uuid.UUID `gorm:"type:uuid;primaryKey"`
	ProjectID             uuid.UUID
	TokenPrefix           string
	TokenHash             []byte
	Name                  string
	Scopes                pq.StringArray `gorm:"type:text[]"`
	Disabled              bool
	ExpiresAt, LastUsedAt *time.Time
	CreatedAt, UpdatedAt  time.Time
}

func (tokenModel) TableName() string { return "api_tokens" }

func (r *Repository) CreateProject(ctx context.Context, p domain.Project) (domain.Project, error) {
	m := projectModel{p.ID, p.Name, p.Status, p.TaskQuota, p.MaxConcurrentTasks, p.CreatedAt, p.UpdatedAt}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		return domain.Project{}, err
	}
	return modelProject(m), nil
}
func (r *Repository) ListProjects(ctx context.Context) ([]domain.Project, error) {
	var rows []projectModel
	if err := r.db.WithContext(ctx).Order("created_at DESC, id DESC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]domain.Project, len(rows))
	for i := range rows {
		out[i] = modelProject(rows[i])
	}
	return out, nil
}
func (r *Repository) GetProject(ctx context.Context, id uuid.UUID) (domain.Project, error) {
	var m projectModel
	err := r.db.WithContext(ctx).First(&m, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Project{}, scheduler.ErrNotFound
	}
	return modelProject(m), err
}
func (r *Repository) UpdateProject(ctx context.Context, id uuid.UUID, changes map[string]any) (domain.Project, error) {
	result := r.db.WithContext(ctx).Model(&projectModel{}).Where("id = ?", id).Updates(changes)
	if result.Error != nil {
		return domain.Project{}, result.Error
	}
	if result.RowsAffected == 0 {
		return domain.Project{}, scheduler.ErrNotFound
	}
	return r.GetProject(ctx, id)
}
func modelProject(m projectModel) domain.Project {
	return domain.Project{ID: m.ID, Name: m.Name, Status: m.Status, TaskQuota: m.TaskQuota, MaxConcurrentTasks: m.MaxConcurrentTasks, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
}

func (r *Repository) CreateToken(ctx context.Context, t domain.APIToken) (domain.APIToken, error) {
	m := tokenModel{ID: t.ID, ProjectID: t.ProjectID, TokenPrefix: t.TokenPrefix, TokenHash: t.TokenHash, Name: t.Name, Scopes: pq.StringArray(t.Scopes), Disabled: t.Disabled, ExpiresAt: t.ExpiresAt, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
	if err := r.db.WithContext(ctx).Create(&m).Error; err != nil {
		return domain.APIToken{}, err
	}
	return modelToken(m), nil
}
func (r *Repository) ListTokens(ctx context.Context, projectID uuid.UUID) ([]domain.APIToken, error) {
	var rows []tokenModel
	if err := r.db.WithContext(ctx).Where("project_id = ?", projectID).Order("created_at DESC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]domain.APIToken, len(rows))
	for i := range rows {
		out[i] = modelToken(rows[i])
		out[i].TokenHash = nil
	}
	return out, nil
}
func (r *Repository) FindActiveTokensByPrefix(ctx context.Context, prefix string) ([]domain.APIToken, error) {
	var rows []tokenModel
	now := time.Now().UTC()
	if err := r.db.WithContext(ctx).Where("token_prefix = ? AND disabled = false AND (expires_at IS NULL OR expires_at > ?)", prefix, now).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]domain.APIToken, len(rows))
	for i := range rows {
		out[i] = modelToken(rows[i])
	}
	return out, nil
}
func (r *Repository) DisableToken(ctx context.Context, projectID, tokenID uuid.UUID) error {
	result := r.db.WithContext(ctx).Model(&tokenModel{}).Where("id=? AND project_id=?", tokenID, projectID).Updates(map[string]any{"disabled": true, "updated_at": time.Now().UTC()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return scheduler.ErrNotFound
	}
	return nil
}
func (r *Repository) TouchToken(ctx context.Context, id uuid.UUID) error {
	return r.db.WithContext(ctx).Model(&tokenModel{}).Where("id=?", id).Update("last_used_at", time.Now().UTC()).Error
}
func modelToken(m tokenModel) domain.APIToken {
	return domain.APIToken{ID: m.ID, ProjectID: m.ProjectID, TokenPrefix: m.TokenPrefix, TokenHash: m.TokenHash, Name: m.Name, Scopes: []string(m.Scopes), Disabled: m.Disabled, ExpiresAt: m.ExpiresAt, LastUsedAt: m.LastUsedAt, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
}

type TaskFilter struct {
	Status, TaskType            string
	JobID                       *uuid.UUID
	Priority                    *int
	CreatedAfter, CreatedBefore *time.Time
	AvailableBefore             *time.Time
	CursorCreatedAt             *time.Time
	CursorID                    *uuid.UUID
	Limit                       int
}
type JobFilter struct {
	Limit           int
	CursorCreatedAt *time.Time
	CursorID        *uuid.UUID
}
type taskRow struct {
	ID, ProjectID          uuid.UUID
	JobID                  *uuid.UUID
	TaskType               string
	Payload                json.RawMessage
	PayloadHash            []byte
	Status                 domain.TaskStatus
	Priority               int
	AvailableAt            time.Time
	ExecutionTimeoutMS     int64
	OverallDeadline        *time.Time
	MaxAttempts, AttemptNo int
	CancelRequestedAt      *time.Time
	Result                 json.RawMessage
	ResultHash             []byte
	FinalErrorType         domain.ErrorType
	FinalErrorMessage      string
	CreatedAt, UpdatedAt   time.Time
	CompletedAt            *time.Time
}

func (r *Repository) ListTasks(ctx context.Context, projectID uuid.UUID, f TaskFilter) ([]domain.Task, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	query := r.db.WithContext(ctx).Table("tasks").Select(`id,project_id,job_id,task_type,status,priority,available_at,(extract(epoch from execution_timeout)*1000)::bigint AS execution_timeout_ms,overall_deadline,max_attempts,attempt_no,cancel_requested_at,created_at,updated_at,completed_at`).Where("project_id=?", projectID)
	if f.Status != "" {
		query = query.Where("status=?", f.Status)
	}
	if f.TaskType != "" {
		query = query.Where("task_type=?", f.TaskType)
	}
	if f.JobID != nil {
		query = query.Where("job_id=?", *f.JobID)
	}
	if f.Priority != nil {
		query = query.Where("priority=?", *f.Priority)
	}
	if f.CreatedAfter != nil {
		query = query.Where("created_at>=?", *f.CreatedAfter)
	}
	if f.CreatedBefore != nil {
		query = query.Where("created_at<?", *f.CreatedBefore)
	}
	if f.AvailableBefore != nil {
		query = query.Where("available_at<=?", *f.AvailableBefore)
	}
	if f.CursorCreatedAt != nil && f.CursorID != nil {
		query = query.Where("(created_at,id)<(?,?)", *f.CursorCreatedAt, *f.CursorID)
	}
	var rows []taskRow
	if err := query.Order("created_at DESC,id DESC").Limit(f.Limit).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return taskRows(rows), nil
}
func (r *Repository) GetTask(ctx context.Context, projectID, taskID uuid.UUID) (domain.Task, error) {
	var row taskRow
	err := r.db.WithContext(ctx).Table("tasks").Select(`id,project_id,job_id,task_type,payload,payload_hash,status,priority,available_at,(extract(epoch from execution_timeout)*1000)::bigint AS execution_timeout_ms,overall_deadline,max_attempts,attempt_no,cancel_requested_at,result,result_hash,final_error_type,final_error_message,created_at,updated_at,completed_at`).Where("project_id=? AND id=?", projectID, taskID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Task{}, scheduler.ErrNotFound
	}
	if err != nil {
		return domain.Task{}, err
	}
	return taskRows([]taskRow{row})[0], nil
}
func taskRows(rows []taskRow) []domain.Task {
	out := make([]domain.Task, len(rows))
	for i, m := range rows {
		t := domain.Task{ID: m.ID, ProjectID: m.ProjectID, JobID: m.JobID, TaskType: m.TaskType, Payload: m.Payload, Status: m.Status, Priority: m.Priority, AvailableAt: m.AvailableAt, ExecutionTimeout: time.Duration(m.ExecutionTimeoutMS) * time.Millisecond, OverallDeadline: m.OverallDeadline, MaxAttempts: m.MaxAttempts, AttemptNo: m.AttemptNo, CancelRequestedAt: m.CancelRequestedAt, Result: m.Result, ResultHash: m.ResultHash, FinalErrorType: m.FinalErrorType, FinalErrorMessage: m.FinalErrorMessage, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt, CompletedAt: m.CompletedAt}
		copy(t.PayloadHash[:], m.PayloadHash)
		out[i] = t
	}
	return out
}

type jobRow struct {
	ID, ProjectID                                        uuid.UUID
	Name                                                 string
	CancelRequestedAt                                    *time.Time
	Metadata                                             json.RawMessage
	CreatedAt, UpdatedAt                                 time.Time
	Total, Pending, Running, Succeeded, Failed, Canceled int64
}

func (r *Repository) ListJobs(ctx context.Context, projectID uuid.UUID, filter JobFilter) ([]domain.Job, []domain.JobCounts, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	var rows []jobRow
	query := `SELECT j.id,j.project_id,j.name,j.cancel_requested_at,j.metadata,j.created_at,j.updated_at,count(t.id) total,count(*) FILTER(WHERE t.status='PENDING') pending,count(*) FILTER(WHERE t.status='RUNNING') running,count(*) FILTER(WHERE t.status='SUCCEEDED') succeeded,count(*) FILTER(WHERE t.status='FAILED') failed,count(*) FILTER(WHERE t.status='CANCELED') canceled FROM jobs j LEFT JOIN tasks t ON t.job_id=j.id WHERE j.project_id=?`
	args := []any{projectID}
	if filter.CursorCreatedAt != nil && filter.CursorID != nil {
		query += ` AND (j.created_at,j.id)<(?,?)`
		args = append(args, *filter.CursorCreatedAt, *filter.CursorID)
	}
	query += ` GROUP BY j.id ORDER BY j.created_at DESC,j.id DESC LIMIT ?`
	args = append(args, filter.Limit)
	err := r.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error
	if err != nil {
		return nil, nil, err
	}
	jobs := make([]domain.Job, len(rows))
	counts := make([]domain.JobCounts, len(rows))
	for i, m := range rows {
		jobs[i] = domain.Job{ID: m.ID, ProjectID: m.ProjectID, Name: m.Name, CancelRequestedAt: m.CancelRequestedAt, Metadata: m.Metadata, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
		counts[i] = domain.JobCounts{Total: m.Total, Pending: m.Pending, Running: m.Running, Succeeded: m.Succeeded, Failed: m.Failed, Canceled: m.Canceled}
	}
	return jobs, counts, nil
}
func (r *Repository) GetJob(ctx context.Context, projectID, jobID uuid.UUID) (domain.Job, domain.JobCounts, error) {
	jobs, counts, err := r.listOneJob(ctx, projectID, jobID)
	if err != nil {
		return domain.Job{}, domain.JobCounts{}, err
	}
	if len(jobs) == 0 {
		return domain.Job{}, domain.JobCounts{}, scheduler.ErrNotFound
	}
	return jobs[0], counts[0], nil
}
func (r *Repository) listOneJob(ctx context.Context, projectID, jobID uuid.UUID) ([]domain.Job, []domain.JobCounts, error) {
	var rows []jobRow
	err := r.db.WithContext(ctx).Raw(`SELECT j.id,j.project_id,j.name,j.cancel_requested_at,j.metadata,j.created_at,j.updated_at,count(t.id) total,count(*) FILTER(WHERE t.status='PENDING') pending,count(*) FILTER(WHERE t.status='RUNNING') running,count(*) FILTER(WHERE t.status='SUCCEEDED') succeeded,count(*) FILTER(WHERE t.status='FAILED') failed,count(*) FILTER(WHERE t.status='CANCELED') canceled FROM jobs j LEFT JOIN tasks t ON t.job_id=j.id WHERE j.project_id=? AND j.id=? GROUP BY j.id`, projectID, jobID).Scan(&rows).Error
	if err != nil {
		return nil, nil, err
	}
	jobs := make([]domain.Job, len(rows))
	counts := make([]domain.JobCounts, len(rows))
	for i, m := range rows {
		jobs[i] = domain.Job{ID: m.ID, ProjectID: m.ProjectID, Name: m.Name, CancelRequestedAt: m.CancelRequestedAt, Metadata: m.Metadata, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
		counts[i] = domain.JobCounts{Total: m.Total, Pending: m.Pending, Running: m.Running, Succeeded: m.Succeeded, Failed: m.Failed, Canceled: m.Canceled}
	}
	return jobs, counts, nil
}

type attemptRow struct {
	TaskID              uuid.UUID
	AttemptNo           int
	WorkerName          string
	WorkerInstanceID    uuid.UUID
	StartedAt           time.Time
	FinishedAt          *time.Time
	Outcome             *domain.TaskOutcome
	ErrorType           domain.ErrorType
	ErrorMessage        string
	ExecutionDurationMS *int64
	LeaseExpired        bool
	ResultHash          []byte
}

func (r *Repository) ListAttempts(ctx context.Context, projectID, taskID uuid.UUID) ([]domain.TaskAttempt, error) {
	var rows []attemptRow
	err := r.db.WithContext(ctx).Table("task_attempts a").Select("a.*").Joins("JOIN tasks t ON t.id=a.task_id").Where("t.project_id=? AND a.task_id=?", projectID, taskID).Order("a.attempt_no DESC").Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]domain.TaskAttempt, len(rows))
	for i, m := range rows {
		out[i] = domain.TaskAttempt{TaskID: m.TaskID, AttemptNo: m.AttemptNo, WorkerName: m.WorkerName, WorkerInstanceID: m.WorkerInstanceID, StartedAt: m.StartedAt, FinishedAt: m.FinishedAt, Outcome: m.Outcome, ErrorType: m.ErrorType, ErrorMessage: m.ErrorMessage, ExecutionDurationMS: m.ExecutionDurationMS, LeaseExpired: m.LeaseExpired, ResultHash: m.ResultHash}
	}
	return out, nil
}

type agentStepRow struct {
	TaskID                      uuid.UUID
	AttemptNo, StepNo           int
	WorkerInstanceID            uuid.UUID
	Kind, ToolName, Status      string
	InputSummary, OutputSummary json.RawMessage
	StartedAt                   time.Time
	FinishedAt                  *time.Time
	CreatedAt, UpdatedAt        time.Time
}

func (r *Repository) ListAgentSteps(ctx context.Context, projectID, taskID uuid.UUID) ([]domain.AgentStep, error) {
	var rows []agentStepRow
	err := r.db.WithContext(ctx).Table("agent_steps s").Select("s.*").Joins("JOIN tasks t ON t.id=s.task_id").Where("t.project_id=? AND s.task_id=?", projectID, taskID).Order("s.attempt_no ASC,s.step_no ASC").Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	steps := make([]domain.AgentStep, len(rows))
	for i, row := range rows {
		steps[i] = domain.AgentStep{TaskID: row.TaskID, AttemptNo: row.AttemptNo, StepNo: row.StepNo, WorkerInstanceID: row.WorkerInstanceID, Kind: row.Kind, ToolName: row.ToolName, InputSummary: row.InputSummary, OutputSummary: row.OutputSummary, Status: row.Status, StartedAt: row.StartedAt, FinishedAt: row.FinishedAt, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
	}
	return steps, nil
}
