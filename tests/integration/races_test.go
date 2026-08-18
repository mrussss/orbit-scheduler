//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/command"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestConcurrentReapersProcessEachLeaseOnce(t *testing.T) {
	ctx := context.Background()
	pool, store, projectID, workerID := raceStore(t, ctx, 12)
	defer pool.Close()
	for index := range 12 {
		_, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "race", Payload: []byte(`{"n":1}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 3, IdempotencyKey: uuid.NewString()})
		if err != nil {
			t.Fatalf("create task %d: %v", index, err)
		}
	}
	assignments, err := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: workerID, Requested: 12, LeaseDuration: time.Second})
	if err != nil || len(assignments) != 12 {
		t.Fatalf("fetch=%d err=%v", len(assignments), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET lease_expires_at=now()-interval '1 second' WHERE project_id=$1 AND status='RUNNING'`, projectID); err != nil {
		t.Fatal(err)
	}

	results := make(chan scheduler.ReapResult, 4)
	errors := make(chan error, 4)
	var wait sync.WaitGroup
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, reapErr := store.ReapExpired(ctx, 3)
			if reapErr != nil {
				errors <- reapErr
				return
			}
			results <- result
		}()
	}
	wait.Wait()
	close(results)
	close(errors)
	for reapErr := range errors {
		t.Fatal(reapErr)
	}
	total := 0
	for result := range results {
		total += result.Requeued + result.Failed + result.Canceled
	}
	if total != 12 {
		t.Fatalf("processed=%d, want 12", total)
	}
	var pending, expiredAttempts, events, projectRunning, workerRunning int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE project_id=$1 AND status='PENDING'`, projectID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE lease_expired`).Scan(&expiredAttempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_type='TASK' AND event_type='TASK_LEASE_EXPIRED' AND aggregate_id IN (SELECT id FROM tasks WHERE project_id=$1)`, projectID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT running_tasks FROM projects WHERE id=$1`, projectID).Scan(&projectRunning); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT running_tasks FROM worker_instances WHERE id=$1`, workerID).Scan(&workerRunning); err != nil {
		t.Fatal(err)
	}
	if pending != 12 || expiredAttempts != 12 || events != 12 || projectRunning != 0 || workerRunning != 0 {
		t.Fatalf("pending=%d expired=%d events=%d project_running=%d worker_running=%d", pending, expiredAttempts, events, projectRunning, workerRunning)
	}
}

func TestReaperAndCompleteRaceKeepsSingleAuthoritativeTransition(t *testing.T) {
	ctx := context.Background()
	pool, store, projectID, workerID := raceStore(t, ctx, 1)
	defer pool.Close()
	created, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "race", Payload: []byte(`{}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	assignment := fetchOne(t, ctx, store, workerID)
	now := time.Now().UTC()
	result := []byte(`{"ok":true}`)
	report := scheduler.ReportRequest{TaskID: assignment.TaskID, WorkerInstanceID: workerID, AttemptNo: assignment.AttemptNo, Outcome: domain.OutcomeSucceeded, Result: result, ResultHash: domain.HashBytes(result), ExecutionStartedAt: now.Add(-time.Millisecond), ExecutionFinishedAt: now}

	start := make(chan struct{})
	completeErr := make(chan error, 1)
	reapErr := make(chan error, 1)
	go func() {
		<-start
		_, reportErr := store.ReportResult(ctx, report)
		completeErr <- reportErr
	}()
	go func() {
		<-start
		_, expireErr := pool.Exec(ctx, `UPDATE tasks SET lease_expires_at=now()-interval '1 second' WHERE id=$1 AND status='RUNNING'`, assignment.TaskID)
		if expireErr == nil {
			_, expireErr = store.ReapExpired(ctx, 1)
		}
		reapErr <- expireErr
	}()
	close(start)
	reportErr := <-completeErr
	if err := <-reapErr; err != nil {
		t.Fatal(err)
	}
	var status string
	var running int
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id=$1`, created.Task.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT running_tasks FROM projects WHERE id=$1`, projectID).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" && status != "PENDING" {
		t.Fatalf("status=%s report_err=%v", status, reportErr)
	}
	if status == "SUCCEEDED" && reportErr != nil {
		t.Fatalf("successful final state had report error: %v", reportErr)
	}
	if status == "PENDING" && reportErr == nil {
		t.Fatal("reaper state and completion both reported success")
	}
	if running != 0 {
		t.Fatalf("project running_tasks=%d", running)
	}
	var finishedAttempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND finished_at IS NOT NULL`, assignment.TaskID).Scan(&finishedAttempts); err != nil {
		t.Fatal(err)
	}
	if finishedAttempts != 1 {
		t.Fatalf("finished attempts=%d", finishedAttempts)
	}
}

func TestCancelAndCompleteRaceDoesNotRegressTerminalState(t *testing.T) {
	ctx := context.Background()
	pool, store, projectID, workerID := raceStore(t, ctx, 1)
	defer pool.Close()
	created, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "race", Payload: []byte(`{}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	assignment := fetchOne(t, ctx, store, workerID)
	now := time.Now().UTC()
	result := []byte(`{"ok":true}`)
	report := scheduler.ReportRequest{TaskID: assignment.TaskID, WorkerInstanceID: workerID, AttemptNo: assignment.AttemptNo, Outcome: domain.OutcomeSucceeded, Result: result, ResultHash: domain.HashBytes(result), ExecutionStartedAt: now.Add(-time.Millisecond), ExecutionFinishedAt: now}
	start := make(chan struct{})
	errors := make(chan error, 2)
	go func() { <-start; errors <- store.CancelTask(ctx, projectID, assignment.TaskID) }()
	go func() { <-start; _, err := store.ReportResult(ctx, report); errors <- err }()
	close(start)
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id=$1`, created.Task.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("status regressed to %s", status)
	}
	if err := store.CancelTask(ctx, projectID, assignment.TaskID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id=$1`, created.Task.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("terminal status regressed to %s", status)
	}
}

func TestPendingCancelAndFetchRaceConvergesToCanceled(t *testing.T) {
	ctx := context.Background()
	pool, store, projectID, workerID := raceStore(t, ctx, 1)
	defer pool.Close()
	created, err := store.CreateTask(ctx, command.CreateTask{ProjectID: projectID, TaskType: "race", Payload: []byte(`{}`), AvailableAt: time.Now(), ExecutionTimeout: time.Second, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	cancelErr := make(chan error, 1)
	fetched := make(chan []scheduler.Assignment, 1)
	fetchErr := make(chan error, 1)
	go func() { <-start; cancelErr <- store.CancelTask(ctx, projectID, created.Task.ID) }()
	go func() {
		<-start
		assignments, err := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: workerID, Requested: 1, LeaseDuration: time.Second})
		fetched <- assignments
		fetchErr <- err
	}()
	close(start)
	if err := <-cancelErr; err != nil {
		t.Fatal(err)
	}
	assignments := <-fetched
	if err := <-fetchErr; err != nil {
		t.Fatal(err)
	}
	if len(assignments) > 1 {
		t.Fatalf("assignments=%d", len(assignments))
	}
	if len(assignments) == 1 {
		renewed, err := store.RenewLease(ctx, scheduler.RenewRequest{TaskID: assignments[0].TaskID, WorkerInstanceID: workerID, AttemptNo: assignments[0].AttemptNo, Extension: time.Second})
		if err != nil || !renewed.CancelRequested {
			t.Fatalf("renew=%+v err=%v", renewed, err)
		}
		now := time.Now().UTC()
		_, err = store.ReportResult(ctx, scheduler.ReportRequest{TaskID: assignments[0].TaskID, WorkerInstanceID: workerID, AttemptNo: assignments[0].AttemptNo, Outcome: domain.OutcomeCanceled, ResultHash: domain.HashBytes(nil), ErrorType: domain.ErrorCanceled, ErrorMessage: "canceled", ExecutionStartedAt: now.Add(-time.Millisecond), ExecutionFinishedAt: now})
		if err != nil {
			t.Fatal(err)
		}
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id=$1`, created.Task.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELED" {
		t.Fatalf("status=%s", status)
	}
}

func raceStore(t *testing.T, ctx context.Context, capacity int) (*pgxpool.Pool, *pgstore.Store, uuid.UUID, uuid.UUID) {
	t.Helper()
	pool, err := pgxpool.New(ctx, migratedPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	store, err := pgstore.New(pool, pgstore.Config{MaxFetchBatch: 20, RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond})
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	projectID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES($1,$2,'ACTIVE',1000,$3,now(),now())`, projectID, uuid.NewString(), capacity); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workerID := uuid.New()
	if err := store.RegisterWorker(ctx, domain.WorkerInstance{ID: workerID, WorkerName: uuid.NewString(), Hostname: "test", Capacity: capacity, SupportedTaskTypes: []string{"race"}, ProcessVersion: "test"}); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool, store, projectID, workerID
}
