package tests

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/idempotency"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestConcurrentIdempotentCreation(t *testing.T) {
	environment := testkit.StartMySQL(t)
	runner, err := migration.New(environment.MigrationDSN, environment.MigrationsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Up(); err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := database.Open(ctx, environment.Config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projectID := insertIsolationProject(t, ctx, db.SQL, "idempotency")
	service, err := idempotency.New(db.SQL)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"kind":"idempotent","value":42}`)
	hash := idempotency.HashRequest(payload)
	key := "concurrent-create"
	now := time.Now().UTC().Truncate(time.Microsecond)
	start := make(chan struct{})
	results := make(chan idempotency.Result, 20)
	errorsCh := make(chan error, 20)
	var callers sync.WaitGroup
	for i := 0; i < 20; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			task := model.Task{ID: model.BinaryUUIDFrom(uuid.New()), ProjectID: model.BinaryUUIDFrom(projectID), IdempotencyKey: &key, RequestHash: hash[:], Status: model.TaskPending, AvailableAt: now, Payload: payload, CreatedAt: now, UpdatedAt: now}
			result, err := service.Create(ctx, task)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	callers.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	var canonical uuid.UUID
	created := 0
	responses := 0
	for result := range results {
		responses++
		if canonical == uuid.Nil {
			canonical = result.TaskID
		}
		if result.TaskID != canonical {
			t.Fatalf("different task IDs: %s and %s", canonical, result.TaskID)
		}
		if result.Created {
			created++
		}
	}
	if responses != 20 || created != 1 {
		t.Fatalf("responses=%d created=%d", responses, created)
	}
	var count int
	if err := db.SQL.QueryRowContext(ctx, `SELECT count(*) FROM lab_tasks WHERE project_id=? AND idempotency_key=?`, model.UUIDToBytes(projectID), key).Scan(&count); err != nil || count != 1 {
		t.Fatalf("rows=%d err=%v", count, err)
	}
	conflictPayload := json.RawMessage(`{"kind":"idempotent","value":43}`)
	conflictHash := idempotency.HashRequest(conflictPayload)
	conflictTask := model.Task{ID: model.BinaryUUIDFrom(uuid.New()), ProjectID: model.BinaryUUIDFrom(projectID), IdempotencyKey: &key, RequestHash: conflictHash[:], Status: model.TaskPending, AvailableAt: now, Payload: conflictPayload, CreatedAt: now, UpdatedAt: now}
	if _, err := service.Create(ctx, conflictTask); !errors.Is(err, idempotency.ErrConflict) {
		t.Fatalf("conflicting request err=%v", err)
	}
}
