package pgstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type normalizedTask struct {
	JobID              *uuid.UUID      `json:"job_id,omitempty"`
	TaskType           string          `json:"task_type"`
	Payload            json.RawMessage `json:"payload"`
	Priority           int             `json:"priority"`
	AvailableAt        string          `json:"available_at"`
	ExecutionTimeoutMS int64           `json:"execution_timeout_ms"`
	OverallDeadline    *string         `json:"overall_deadline,omitempty"`
	MaxAttempts        int             `json:"max_attempts"`
}

func normalizeTask(input command.CreateTask) (normalizedTask, [32]byte, [32]byte, error) {
	canonical, err := domain.CanonicalJSON(input.Payload)
	if err != nil {
		return normalizedTask{}, [32]byte{}, [32]byte{}, fmt.Errorf("%w: payload: %v", command.ErrInvalidCreate, err)
	}
	if input.TaskType == "" || input.ExecutionTimeout <= 0 || input.MaxAttempts <= 0 || input.AvailableAt.IsZero() {
		return normalizedTask{}, [32]byte{}, [32]byte{}, command.ErrInvalidCreate
	}
	var deadline *string
	if input.OverallDeadline != nil {
		value := input.OverallDeadline.UTC().Format(time.RFC3339Nano)
		deadline = &value
	}
	normalized := normalizedTask{JobID: input.JobID, TaskType: input.TaskType, Payload: canonical, Priority: input.Priority, AvailableAt: input.AvailableAt.UTC().Format(time.RFC3339Nano), ExecutionTimeoutMS: input.ExecutionTimeout.Milliseconds(), OverallDeadline: deadline, MaxAttempts: input.MaxAttempts}
	body, _ := json.Marshal(normalized)
	return normalized, domain.HashBytes(body), domain.HashBytes(canonical), nil
}

func (s *Store) CreateTask(ctx context.Context, input command.CreateTask) (command.CreatedTask, error) {
	normalized, requestHash, payloadHash, err := normalizeTask(input)
	if err != nil {
		return command.CreatedTask{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return command.CreatedTask{}, fmt.Errorf("create task begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	projectStatus, quota, count, err := lockProject(ctx, tx, input.ProjectID)
	if err != nil {
		return command.CreatedTask{}, err
	}
	if input.IdempotencyKey != "" {
		existing, hash, found, err := findTaskByKey(ctx, tx, input.ProjectID, input.IdempotencyKey)
		if err != nil {
			return command.CreatedTask{}, err
		}
		if found {
			if !bytes.Equal(hash, requestHash[:]) {
				return command.CreatedTask{}, command.ErrIdempotencyConflict
			}
			if err := tx.Commit(ctx); err != nil {
				return command.CreatedTask{}, err
			}
			return command.CreatedTask{Task: existing, Created: false}, nil
		}
	}
	if input.JobID != nil {
		var belongs bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1 AND project_id=$2)`, *input.JobID, input.ProjectID).Scan(&belongs); err != nil {
			return command.CreatedTask{}, err
		}
		if !belongs {
			return command.CreatedTask{}, scheduler.ErrNotFound
		}
	}
	if projectStatus != domain.ProjectActive {
		return command.CreatedTask{}, command.ErrProjectDisabled
	}
	if count+1 > quota {
		return command.CreatedTask{}, command.ErrQuotaExceeded
	}
	createdAt := time.Now().UTC()
	if err := tx.QueryRow(ctx, `SELECT statement_timestamp()`).Scan(&createdAt); err != nil {
		return command.CreatedTask{}, err
	}
	task := domain.Task{ID: uuid.New(), ProjectID: input.ProjectID, JobID: input.JobID, TaskType: input.TaskType, Payload: normalized.Payload, PayloadHash: payloadHash, Status: domain.TaskPending, Priority: input.Priority, AvailableAt: input.AvailableAt.UTC(), ExecutionTimeout: input.ExecutionTimeout, OverallDeadline: input.OverallDeadline, MaxAttempts: input.MaxAttempts, CreatedAt: createdAt, UpdatedAt: createdAt}
	_, err = tx.Exec(ctx, `INSERT INTO tasks(id,project_id,job_id,task_type,payload,payload_hash,status,priority,available_at,execution_timeout,overall_deadline,max_attempts,idempotency_key,creation_request_hash,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'PENDING',$7,$8,$9::bigint*interval '1 millisecond',$10,$11,$12,$13,statement_timestamp(),statement_timestamp())`, task.ID, task.ProjectID, task.JobID, task.TaskType, task.Payload, task.PayloadHash[:], task.Priority, task.AvailableAt, task.ExecutionTimeout.Milliseconds(), task.OverallDeadline, task.MaxAttempts, nullable(input.IdempotencyKey), optionalHash(input.IdempotencyKey, requestHash))
	if err != nil {
		return command.CreatedTask{}, fmt.Errorf("insert task: %w", err)
	}
	if err := insertCreatedEvent(ctx, tx, task.ID, input.ProjectID, input.TraceID); err != nil {
		return command.CreatedTask{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return command.CreatedTask{}, fmt.Errorf("create task commit: %w", err)
	}
	return command.CreatedTask{Task: task, Created: true}, nil
}

func (s *Store) CreateJob(ctx context.Context, input command.CreateJob) (command.CreatedJob, error) {
	if input.Name == "" || len(input.Tasks) == 0 {
		return command.CreatedJob{}, command.ErrInvalidCreate
	}
	normalizedTasks := make([]normalizedTask, len(input.Tasks))
	payloadHashes := make([][32]byte, len(input.Tasks))
	for i := range input.Tasks {
		if input.Tasks[i].JobID != nil {
			return command.CreatedJob{}, command.ErrInvalidCreate
		}
		input.Tasks[i].ProjectID = input.ProjectID
		n, _, p, err := normalizeTask(input.Tasks[i])
		if err != nil {
			return command.CreatedJob{}, err
		}
		normalizedTasks[i], payloadHashes[i] = n, p
	}
	metadata, err := domain.CanonicalJSON(input.Metadata)
	if err != nil {
		return command.CreatedJob{}, command.ErrInvalidCreate
	}
	hashBody, _ := json.Marshal(struct {
		Name     string           `json:"name"`
		Metadata json.RawMessage  `json:"metadata"`
		Tasks    []normalizedTask `json:"tasks"`
	}{input.Name, metadata, normalizedTasks})
	requestHash := domain.HashBytes(hashBody)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return command.CreatedJob{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	projectStatus, quota, count, err := lockProject(ctx, tx, input.ProjectID)
	if err != nil {
		return command.CreatedJob{}, err
	}
	if input.IdempotencyKey != "" {
		job, hash, found, err := findJobByKey(ctx, tx, input.ProjectID, input.IdempotencyKey)
		if err != nil {
			return command.CreatedJob{}, err
		}
		if found {
			if !bytes.Equal(hash, requestHash[:]) {
				return command.CreatedJob{}, command.ErrIdempotencyConflict
			}
			tasks, err := loadJobTasks(ctx, tx, job.ID)
			if err != nil {
				return command.CreatedJob{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return command.CreatedJob{}, err
			}
			return command.CreatedJob{Job: job, Tasks: tasks}, nil
		}
	}
	if projectStatus != domain.ProjectActive {
		return command.CreatedJob{}, command.ErrProjectDisabled
	}
	if count+int64(len(input.Tasks)) > quota {
		return command.CreatedJob{}, command.ErrQuotaExceeded
	}
	now := time.Now().UTC()
	if err := tx.QueryRow(ctx, `SELECT statement_timestamp()`).Scan(&now); err != nil {
		return command.CreatedJob{}, err
	}
	job := domain.Job{ID: uuid.New(), ProjectID: input.ProjectID, Name: input.Name, Metadata: metadata, CreatedAt: now, UpdatedAt: now}
	_, err = tx.Exec(ctx, `INSERT INTO jobs(id,project_id,name,idempotency_key,creation_request_hash,metadata,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,statement_timestamp(),statement_timestamp())`, job.ID, job.ProjectID, job.Name, nullable(input.IdempotencyKey), optionalHash(input.IdempotencyKey, requestHash), job.Metadata)
	if err != nil {
		return command.CreatedJob{}, fmt.Errorf("insert job: %w", err)
	}
	created := make([]domain.Task, 0, len(input.Tasks))
	for i, item := range input.Tasks {
		item.ProjectID = input.ProjectID
		item.JobID = &job.ID
		n := normalizedTasks[i]
		task := domain.Task{ID: uuid.New(), ProjectID: input.ProjectID, JobID: &job.ID, TaskType: item.TaskType, Payload: n.Payload, PayloadHash: payloadHashes[i], Status: domain.TaskPending, Priority: item.Priority, AvailableAt: item.AvailableAt.UTC(), ExecutionTimeout: item.ExecutionTimeout, OverallDeadline: item.OverallDeadline, MaxAttempts: item.MaxAttempts, CreatedAt: now, UpdatedAt: now}
		_, err = tx.Exec(ctx, `INSERT INTO tasks(id,project_id,job_id,task_type,payload,payload_hash,status,priority,available_at,execution_timeout,overall_deadline,max_attempts,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,'PENDING',$7,$8,$9::bigint*interval '1 millisecond',$10,$11,statement_timestamp(),statement_timestamp())`, task.ID, task.ProjectID, task.JobID, task.TaskType, task.Payload, task.PayloadHash[:], task.Priority, task.AvailableAt, task.ExecutionTimeout.Milliseconds(), task.OverallDeadline, task.MaxAttempts)
		if err != nil {
			return command.CreatedJob{}, fmt.Errorf("insert job task: %w", err)
		}
		if err := insertCreatedEvent(ctx, tx, task.ID, input.ProjectID, input.TraceID); err != nil {
			return command.CreatedJob{}, err
		}
		created = append(created, task)
	}
	payload, _ := json.Marshal(map[string]any{"job_id": job.ID, "project_id": job.ProjectID, "task_count": len(created)})
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,trace_id,created_at,next_attempt_at)VALUES($1,'JOB',$2,'JOB_CREATED',1,$3,$4,$5,statement_timestamp(),statement_timestamp())`, uuid.New(), job.ID, job.ID.String(), payload, nullable(input.TraceID))
	if err != nil {
		return command.CreatedJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return command.CreatedJob{}, err
	}
	return command.CreatedJob{Job: job, Tasks: created, Created: true}, nil
}

func lockProject(ctx context.Context, tx pgx.Tx, projectID uuid.UUID) (domain.ProjectStatus, int64, int64, error) {
	var status domain.ProjectStatus
	var quota, count int64
	err := tx.QueryRow(ctx, `SELECT p.status,p.task_quota,(SELECT count(*) FROM tasks WHERE project_id=p.id) FROM projects p WHERE p.id=$1 FOR UPDATE`, projectID).Scan(&status, &quota, &count)
	if err == pgx.ErrNoRows {
		return "", 0, 0, scheduler.ErrNotFound
	}
	if err != nil {
		return "", 0, 0, err
	}
	return status, quota, count, nil
}

func insertCreatedEvent(ctx context.Context, tx pgx.Tx, taskID, projectID uuid.UUID, traceID string) error {
	payload, _ := json.Marshal(map[string]any{"task_id": taskID, "project_id": projectID})
	_, err := tx.Exec(ctx, `INSERT INTO outbox_events(event_id,aggregate_type,aggregate_id,event_type,event_version,event_key,payload,trace_id,created_at,next_attempt_at)VALUES($1,'TASK',$2,'TASK_CREATED',1,$3,$4,$5,statement_timestamp(),statement_timestamp())`, uuid.New(), taskID, taskID.String(), payload, nullable(traceID))
	return err
}
func optionalHash(key string, hash [32]byte) any {
	if key == "" {
		return nil
	}
	return hash[:]
}

func findTaskByKey(ctx context.Context, tx pgx.Tx, projectID uuid.UUID, key string) (domain.Task, []byte, bool, error) {
	var t domain.Task
	var timeoutMS int64
	var hash, payloadHash []byte
	err := tx.QueryRow(ctx, `SELECT id,project_id,job_id,task_type,payload,payload_hash,status,priority,available_at,(extract(epoch from execution_timeout)*1000)::bigint,overall_deadline,max_attempts,attempt_no,created_at,updated_at,creation_request_hash FROM tasks WHERE project_id=$1 AND idempotency_key=$2`, projectID, key).Scan(&t.ID, &t.ProjectID, &t.JobID, &t.TaskType, &t.Payload, &payloadHash, &t.Status, &t.Priority, &t.AvailableAt, &timeoutMS, &t.OverallDeadline, &t.MaxAttempts, &t.AttemptNo, &t.CreatedAt, &t.UpdatedAt, &hash)
	if err == pgx.ErrNoRows {
		return t, nil, false, nil
	}
	if err != nil {
		return t, nil, false, err
	}
	t.ExecutionTimeout = time.Duration(timeoutMS) * time.Millisecond
	copy(t.PayloadHash[:], payloadHash)
	return t, hash, true, nil
}
func findJobByKey(ctx context.Context, tx pgx.Tx, projectID uuid.UUID, key string) (domain.Job, []byte, bool, error) {
	var job domain.Job
	var hash []byte
	err := tx.QueryRow(ctx, `SELECT id,project_id,name,cancel_requested_at,metadata,created_at,updated_at,creation_request_hash FROM jobs WHERE project_id=$1 AND idempotency_key=$2`, projectID, key).Scan(&job.ID, &job.ProjectID, &job.Name, &job.CancelRequestedAt, &job.Metadata, &job.CreatedAt, &job.UpdatedAt, &hash)
	if err == pgx.ErrNoRows {
		return job, nil, false, nil
	}
	return job, hash, err == nil, err
}
func loadJobTasks(ctx context.Context, tx pgx.Tx, jobID uuid.UUID) ([]domain.Task, error) {
	rows, err := tx.Query(ctx, `SELECT id,project_id,job_id,task_type,payload,payload_hash,status,priority,available_at,(extract(epoch from execution_timeout)*1000)::bigint,overall_deadline,max_attempts,attempt_no,created_at,updated_at FROM tasks WHERE job_id=$1 ORDER BY created_at,id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []domain.Task
	for rows.Next() {
		var t domain.Task
		var timeoutMS int64
		var payloadHash []byte
		if err := rows.Scan(&t.ID, &t.ProjectID, &t.JobID, &t.TaskType, &t.Payload, &payloadHash, &t.Status, &t.Priority, &t.AvailableAt, &timeoutMS, &t.OverallDeadline, &t.MaxAttempts, &t.AttemptNo, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		t.ExecutionTimeout = time.Duration(timeoutMS) * time.Millisecond
		copy(t.PayloadHash[:], payloadHash)
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}
