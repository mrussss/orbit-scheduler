package dataset

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
)

const (
	DefaultProjectCount = 5
	DefaultTaskCount    = 100_000
	DeepPageTaskCount   = 60_100
	batchSize           = 500
)

var namespace = uuid.MustParse("d6f4a292-18db-4e0d-b99f-44f38ab460a0")

type Summary struct {
	ProjectIDs    []uuid.UUID
	DeepProjectID uuid.UUID
	DeepStatus    model.TaskStatus
	TaskCount     int
	DeepTaskCount int
	BaseCreatedAt time.Time
}

func Seed(ctx context.Context, db *sql.DB, seed int64, taskCount int) (Summary, error) {
	if db == nil || taskCount < DeepPageTaskCount {
		return Summary{}, fmt.Errorf("dataset requires a database and at least %d tasks", DeepPageTaskCount)
	}
	projects := make([]uuid.UUID, DefaultProjectCount)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Summary{}, err
	}
	defer func() { _ = tx.Rollback() }()
	for i := range projects {
		projects[i] = deterministicUUID(fmt.Sprintf("project-%d", i))
		if _, err := tx.ExecContext(ctx, `INSERT INTO lab_projects(id,name,status,created_at,updated_at) VALUES(?,?, 'ACTIVE',?,?)`, model.UUIDToBytes(projects[i]), fmt.Sprintf("dataset-%d", i), base, base); err != nil {
			return Summary{}, err
		}
	}
	rng := rand.New(rand.NewSource(seed))
	for start := 0; start < taskCount; start += batchSize {
		end := min(start+batchSize, taskCount)
		var statement strings.Builder
		statement.WriteString(`INSERT INTO lab_tasks(id,project_id,status,priority,available_at,payload,created_at,updated_at) VALUES `)
		args := make([]any, 0, (end-start)*8)
		for i := start; i < end; i++ {
			if i > start {
				statement.WriteByte(',')
			}
			statement.WriteString("(?,?,?,?,?,?,?,?)")
			projectID := projects[0]
			status := model.TaskPending
			if i >= DeepPageTaskCount {
				projectID = projects[1+rng.Intn(len(projects)-1)]
				statuses := []model.TaskStatus{model.TaskPending, model.TaskRunning, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled}
				status = statuses[rng.Intn(len(statuses))]
			}
			createdAt := base.Add(time.Duration(i) * time.Microsecond)
			payload, _ := json.Marshal(map[string]any{"sequence": i, "seed": seed})
			args = append(args,
				model.UUIDToBytes(deterministicUUID(fmt.Sprintf("task-%d-%d", seed, i))),
				model.UUIDToBytes(projectID), status, rng.Intn(101), base, payload, createdAt, createdAt,
			)
		}
		if _, err := tx.ExecContext(ctx, statement.String(), args...); err != nil {
			return Summary{}, fmt.Errorf("insert task batch %d: %w", start/batchSize, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Summary{}, err
	}
	return Summary{ProjectIDs: projects, DeepProjectID: projects[0], DeepStatus: model.TaskPending, TaskCount: taskCount, DeepTaskCount: min(taskCount, DeepPageTaskCount), BaseCreatedAt: base}, nil
}

func deterministicUUID(value string) uuid.UUID {
	return uuid.NewSHA1(namespace, []byte(value))
}
