package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/repository"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestGORMCRUDNativeTransactionsConstraintsAndResources(t *testing.T) {
	environment := testkit.StartMySQL(t)
	environment.Config.TxTimeout = 100 * time.Millisecond
	runner, err := migration.New(environment.MigrationDSN, environment.MigrationsPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Up(); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := database.Open(ctx, environment.Config)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := repository.New(db.GORM)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	projectID := uuid.New()
	project := model.Project{ID: model.BinaryUUIDFrom(projectID), Name: "foundation", Status: model.ProjectActive, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	loadedProject, err := repo.GetProject(ctx, projectID)
	if err != nil || loadedProject.ID.UUID() != projectID || !loadedProject.CreatedAt.Equal(now) {
		t.Fatalf("project=%+v err=%v", loadedProject, err)
	}
	updatedAt := now.Add(time.Second)
	if err := repo.UpdateProject(ctx, projectID, "foundation-updated", model.ProjectDisabled, updatedAt); err != nil {
		t.Fatal(err)
	}
	loadedProject, err = repo.GetProject(ctx, projectID)
	if err != nil || loadedProject.Name != "foundation-updated" || loadedProject.Status != model.ProjectDisabled || !loadedProject.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("updated project=%+v err=%v", loadedProject, err)
	}

	taskID := uuid.New()
	idempotencyKey := "foundation-key"
	hash := make([]byte, 32)
	copy(hash, []byte("stable-request-hash"))
	payload := json.RawMessage(`{"kind":"foundation","nested":{"value":1}}`)
	task := model.Task{ID: model.BinaryUUIDFrom(taskID), ProjectID: model.BinaryUUIDFrom(projectID), IdempotencyKey: &idempotencyKey, RequestHash: hash, Status: model.TaskPending, Priority: 7, AvailableAt: now, Payload: payload, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	loadedTask, err := repo.GetTask(ctx, taskID)
	if err != nil || loadedTask.ID.UUID() != taskID || loadedTask.ProjectID.UUID() != projectID || !jsonEqual(loadedTask.Payload, payload) || !loadedTask.AvailableAt.Equal(now) {
		t.Fatalf("task=%+v err=%v", loadedTask, err)
	}
	duplicate := task
	duplicate.ID = model.BinaryUUIDFrom(uuid.New())
	if err := repo.CreateTask(ctx, duplicate); !errors.Is(err, repository.ErrDuplicate) {
		t.Fatalf("duplicate err=%v", err)
	}

	workerID := uuid.New()
	attempt := model.TaskAttempt{TaskID: model.BinaryUUIDFrom(taskID), AttemptNo: 1, WorkerID: model.BinaryUUIDFrom(workerID), StartedAt: now, CreatedAt: now}
	if err := repo.CreateAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(2 * time.Second)
	if err := repo.FinishAttempt(ctx, taskID, 1, "SUCCEEDED", finishedAt); err != nil {
		t.Fatal(err)
	}
	loadedAttempt, err := repo.GetAttempt(ctx, taskID, 1)
	if err != nil || loadedAttempt.WorkerID.UUID() != workerID || loadedAttempt.FinishedAt == nil || !loadedAttempt.FinishedAt.Equal(finishedAt) || loadedAttempt.Outcome == nil || *loadedAttempt.Outcome != "SUCCEEDED" {
		t.Fatalf("attempt=%+v err=%v", loadedAttempt, err)
	}
	resultJSON := json.RawMessage(`{"ok":true,"source":"mysql8"}`)
	if err := repo.CompleteTask(ctx, taskID, resultJSON, finishedAt); err != nil {
		t.Fatal(err)
	}
	loadedTask, err = repo.GetTask(ctx, taskID)
	if err != nil || loadedTask.Status != model.TaskSucceeded || !jsonEqual(loadedTask.Result, resultJSON) {
		t.Fatalf("completed task=%+v err=%v", loadedTask, err)
	}
	if err := repo.CompleteTask(ctx, taskID, resultJSON, finishedAt); !errors.Is(err, repository.ErrInvalidState) {
		t.Fatalf("repeat complete err=%v", err)
	}

	rollbackErr := errors.New("force rollback")
	err = db.WithinTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(txCtx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txCtx, `UPDATE lab_projects SET name='rolled-back' WHERE id=?`, model.UUIDToBytes(projectID)); err != nil {
			return err
		}
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback err=%v", err)
	}
	loadedProject, err = repo.GetProject(ctx, projectID)
	if err != nil || loadedProject.Name == "rolled-back" {
		t.Fatalf("transaction did not roll back: %+v err=%v", loadedProject, err)
	}
	if err := db.WithinTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(txCtx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txCtx, `UPDATE lab_projects SET name='native-committed' WHERE id=?`, model.UUIDToBytes(projectID))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	loadedProject, err = repo.GetProject(ctx, projectID)
	if err != nil || loadedProject.Name != "native-committed" {
		t.Fatalf("native transaction project=%+v err=%v", loadedProject, err)
	}
	if err := db.WithinTx(context.Background(), nil, func(txCtx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txCtx, `SELECT SLEEP(1)`)
		return err
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transaction timeout err=%v", err)
	}

	missingProjectTask := task
	missingProjectTask.ID = model.BinaryUUIDFrom(uuid.New())
	missingProjectTask.ProjectID = model.BinaryUUIDFrom(uuid.New())
	otherKey := "missing-project"
	missingProjectTask.IdempotencyKey = &otherKey
	if err := repo.CreateTask(ctx, missingProjectTask); !errors.Is(err, repository.ErrConstraint) {
		t.Fatalf("foreign key err=%v", err)
	}
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := repo.GetTask(canceledCtx, taskID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CRUD err=%v", err)
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer timeoutCancel()
	if _, err := db.SQL.ExecContext(timeoutCtx, `SELECT SLEEP(1)`); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("native timeout err=%v", err)
	}

	if err := repo.DeleteProject(ctx, projectID); !errors.Is(err, repository.ErrConstraint) {
		t.Fatalf("delete referenced project err=%v", err)
	}
	if err := repo.DeleteAttempt(ctx, taskID, 1); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteProject(ctx, projectID); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := db.SQL.Stats(); stats.OpenConnections != 0 {
		t.Fatalf("open connections after close=%d", stats.OpenConnections)
	}
	if err := db.Ping(context.Background()); err == nil {
		t.Fatal("closed connection pool still reports healthy")
	}
}

func jsonEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil && valuesEqual(leftValue, rightValue)
}

func valuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}
