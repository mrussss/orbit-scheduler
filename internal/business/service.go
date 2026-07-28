package business

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/auth"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/gormrepo"
)

var (
	ErrUnauthorized    = errors.New("unauthorized")
	ErrForbidden       = errors.New("forbidden")
	ErrInvalidArgument = errors.New("invalid argument")
)
var validScopes = map[string]struct{}{"task:read": {}, "task:write": {}, "job:read": {}, "job:write": {}, "project:admin": {}}

type Principal struct {
	ProjectID, TokenID uuid.UUID
	Scopes             map[string]struct{}
}

func (p Principal) Has(scope string) bool { _, ok := p.Scopes[scope]; return ok }

type Repository interface {
	CreateProject(context.Context, domain.Project) (domain.Project, error)
	ListProjects(context.Context) ([]domain.Project, error)
	GetProject(context.Context, uuid.UUID) (domain.Project, error)
	UpdateProject(context.Context, uuid.UUID, map[string]any) (domain.Project, error)
	CreateToken(context.Context, domain.APIToken) (domain.APIToken, error)
	ListTokens(context.Context, uuid.UUID) ([]domain.APIToken, error)
	FindActiveTokensByPrefix(context.Context, string) ([]domain.APIToken, error)
	DisableToken(context.Context, uuid.UUID, uuid.UUID) error
	TouchToken(context.Context, uuid.UUID) error
	ListTasks(context.Context, uuid.UUID, gormrepo.TaskFilter) ([]domain.Task, error)
	GetTask(context.Context, uuid.UUID, uuid.UUID) (domain.Task, error)
	ListAttempts(context.Context, uuid.UUID, uuid.UUID) ([]domain.TaskAttempt, error)
	ListJobs(context.Context, uuid.UUID, gormrepo.JobFilter) ([]domain.Job, []domain.JobCounts, error)
	GetJob(context.Context, uuid.UUID, uuid.UUID) (domain.Job, domain.JobCounts, error)
}
type Scheduler interface {
	command.Creator
	CancelTask(context.Context, uuid.UUID, uuid.UUID) error
	CancelJob(context.Context, uuid.UUID, uuid.UUID) error
}
type Service struct {
	repo       Repository
	scheduler  Scheduler
	tokens     *auth.TokenCodec
	adminToken string
	touches    chan uuid.UUID
}

func New(repo Repository, scheduler Scheduler, codec *auth.TokenCodec, adminToken string) (*Service, error) {
	if repo == nil || scheduler == nil || codec == nil {
		return nil, errors.New("business dependencies are required")
	}
	if len(adminToken) < 32 {
		return nil, errors.New("admin token too short")
	}
	return &Service{repo: repo, scheduler: scheduler, tokens: codec, adminToken: adminToken, touches: make(chan uuid.UUID, 256)}, nil
}

func (s *Service) RunTokenTouches(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.touches:
			touchCtx, cancel := context.WithTimeout(ctx, time.Second)
			_ = s.repo.TouchToken(touchCtx, id)
			cancel()
		}
	}
}
func (s *Service) IsAdmin(token string) bool {
	return len(token) == len(s.adminToken) && subtle.ConstantTimeCompare([]byte(token), []byte(s.adminToken)) == 1
}
func (s *Service) Authenticate(ctx context.Context, plain string) (Principal, error) {
	prefix, err := auth.Prefix(plain)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	candidates, err := s.repo.FindActiveTokensByPrefix(ctx, prefix)
	if err != nil {
		return Principal{}, fmt.Errorf("authenticate: %w", err)
	}
	hash := s.tokens.Hash(plain)
	for _, candidate := range candidates {
		if auth.Equal(candidate.TokenHash, hash[:]) {
			scopes := make(map[string]struct{}, len(candidate.Scopes))
			for _, scope := range candidate.Scopes {
				scopes[scope] = struct{}{}
			}
			select {
			case s.touches <- candidate.ID:
			default:
			}
			return Principal{ProjectID: candidate.ProjectID, TokenID: candidate.ID, Scopes: scopes}, nil
		}
	}
	return Principal{}, ErrUnauthorized
}

type CreateProjectInput struct {
	Name               string
	TaskQuota          int64
	MaxConcurrentTasks int
}

func (s *Service) CreateProject(ctx context.Context, in CreateProjectInput) (domain.Project, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || len(in.Name) > 200 || in.TaskQuota < 0 || in.MaxConcurrentTasks <= 0 {
		return domain.Project{}, ErrInvalidArgument
	}
	now := time.Now().UTC()
	return s.repo.CreateProject(ctx, domain.Project{ID: uuid.New(), Name: in.Name, Status: domain.ProjectActive, TaskQuota: in.TaskQuota, MaxConcurrentTasks: in.MaxConcurrentTasks, CreatedAt: now, UpdatedAt: now})
}
func (s *Service) ListProjects(ctx context.Context) ([]domain.Project, error) {
	return s.repo.ListProjects(ctx)
}
func (s *Service) GetProject(ctx context.Context, id uuid.UUID) (domain.Project, error) {
	return s.repo.GetProject(ctx, id)
}
func (s *Service) UpdateProject(ctx context.Context, id uuid.UUID, name *string, status *domain.ProjectStatus, quota *int64, maxConcurrent *int) (domain.Project, error) {
	changes := map[string]any{"updated_at": time.Now().UTC()}
	if name != nil {
		value := strings.TrimSpace(*name)
		if value == "" || len(value) > 200 {
			return domain.Project{}, ErrInvalidArgument
		}
		changes["name"] = value
	}
	if status != nil {
		if *status != domain.ProjectActive && *status != domain.ProjectDisabled {
			return domain.Project{}, ErrInvalidArgument
		}
		changes["status"] = *status
	}
	if quota != nil {
		if *quota < 0 {
			return domain.Project{}, ErrInvalidArgument
		}
		changes["task_quota"] = *quota
	}
	if maxConcurrent != nil {
		if *maxConcurrent <= 0 {
			return domain.Project{}, ErrInvalidArgument
		}
		changes["max_concurrent_tasks"] = *maxConcurrent
	}
	return s.repo.UpdateProject(ctx, id, changes)
}

type CreatedToken struct {
	Token     domain.APIToken
	Plaintext string
}

func (s *Service) CreateToken(ctx context.Context, projectID uuid.UUID, name string, scopes []string, expires *time.Time) (CreatedToken, error) {
	if _, err := s.repo.GetProject(ctx, projectID); err != nil {
		return CreatedToken{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" || len(scopes) == 0 {
		return CreatedToken{}, ErrInvalidArgument
	}
	if expires != nil && !expires.After(time.Now()) {
		return CreatedToken{}, ErrInvalidArgument
	}
	seen := map[string]struct{}{}
	for _, scope := range scopes {
		if _, ok := validScopes[scope]; !ok {
			return CreatedToken{}, ErrInvalidArgument
		}
		seen[scope] = struct{}{}
	}
	scopes = scopes[:0]
	for scope := range seen {
		scopes = append(scopes, scope)
	}
	plain, prefix, hash, err := s.tokens.Generate()
	if err != nil {
		return CreatedToken{}, err
	}
	now := time.Now().UTC()
	token, err := s.repo.CreateToken(ctx, domain.APIToken{ID: uuid.New(), ProjectID: projectID, TokenPrefix: prefix, TokenHash: hash[:], Name: name, Scopes: scopes, ExpiresAt: expires, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		return CreatedToken{}, err
	}
	token.TokenHash = nil
	return CreatedToken{token, plain}, nil
}
func (s *Service) ListTokens(ctx context.Context, projectID uuid.UUID) ([]domain.APIToken, error) {
	return s.repo.ListTokens(ctx, projectID)
}
func (s *Service) DisableToken(ctx context.Context, projectID, tokenID uuid.UUID) error {
	return s.repo.DisableToken(ctx, projectID, tokenID)
}

func (s *Service) CreateTask(ctx context.Context, p Principal, input command.CreateTask) (command.CreatedTask, error) {
	input.ProjectID = p.ProjectID
	return s.scheduler.CreateTask(ctx, input)
}
func (s *Service) CreateJob(ctx context.Context, p Principal, input command.CreateJob) (command.CreatedJob, error) {
	input.ProjectID = p.ProjectID
	for i := range input.Tasks {
		input.Tasks[i].ProjectID = p.ProjectID
	}
	return s.scheduler.CreateJob(ctx, input)
}
func (s *Service) ListTasks(ctx context.Context, p Principal, filter gormrepo.TaskFilter) ([]domain.Task, error) {
	return s.repo.ListTasks(ctx, p.ProjectID, filter)
}
func (s *Service) GetTask(ctx context.Context, p Principal, id uuid.UUID) (domain.Task, error) {
	return s.repo.GetTask(ctx, p.ProjectID, id)
}
func (s *Service) ListAttempts(ctx context.Context, p Principal, id uuid.UUID) ([]domain.TaskAttempt, error) {
	if _, err := s.repo.GetTask(ctx, p.ProjectID, id); err != nil {
		return nil, err
	}
	return s.repo.ListAttempts(ctx, p.ProjectID, id)
}
func (s *Service) CancelTask(ctx context.Context, p Principal, id uuid.UUID) error {
	return s.scheduler.CancelTask(ctx, p.ProjectID, id)
}
func (s *Service) ListJobs(ctx context.Context, p Principal, filter gormrepo.JobFilter) ([]domain.Job, []domain.JobCounts, error) {
	return s.repo.ListJobs(ctx, p.ProjectID, filter)
}
func (s *Service) GetJob(ctx context.Context, p Principal, id uuid.UUID) (domain.Job, domain.JobCounts, error) {
	return s.repo.GetJob(ctx, p.ProjectID, id)
}
func (s *Service) CancelJob(ctx context.Context, p Principal, id uuid.UUID) error {
	return s.scheduler.CancelJob(ctx, p.ProjectID, id)
}
