package tests

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/scheduler"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestSkipLockedConcurrentClaimsAndRollback(t *testing.T) {
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
	projectID := insertIsolationProject(t, ctx, db.SQL, "skip-locked")
	insertClaimTasks(t, ctx, db.SQL, projectID, 100)
	claimer, err := scheduler.New(db.SQL)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	claimedCh := make(chan scheduler.ClaimedTask, 100)
	errCh := make(chan error, 8)
	var claimedCount atomic.Int64
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			workerID := uuid.New()
			for {
				tasks, err := claimer.Claim(ctx, scheduler.ClaimRequest{WorkerID: workerID, Limit: 7, LeaseDuration: time.Minute})
				if err != nil {
					errCh <- fmt.Errorf("worker %d: %w", worker, err)
					return
				}
				if len(tasks) == 0 {
					// An empty SKIP LOCKED result only means that no row is
					// available to this transaction right now. Other claimers may
					// still hold every remaining candidate, so it is not a global
					// completion signal.
					if claimedCount.Load() >= 100 {
						return
					}
					select {
					case <-ctx.Done():
						errCh <- fmt.Errorf("worker %d waiting for candidates: %w", worker, ctx.Err())
						return
					case <-time.After(5 * time.Millisecond):
						continue
					}
				}
				for _, task := range tasks {
					claimedCh <- task
					claimedCount.Add(1)
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	close(claimedCh)
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	seen := make(map[uuid.UUID]struct{}, 100)
	for task := range claimedCh {
		if _, duplicate := seen[task.ID]; duplicate {
			t.Fatalf("task claimed twice: %s", task.ID)
		}
		if task.AttemptNo != 1 || task.LeaseExpiresAt.IsZero() {
			t.Fatalf("invalid claim: %+v", task)
		}
		seen[task.ID] = struct{}{}
	}
	if len(seen) != 100 {
		t.Fatalf("claimed=%d want=100", len(seen))
	}
	var running, attempts int
	if err := db.SQL.QueryRowContext(ctx, `SELECT count(*) FROM lab_tasks WHERE project_id=? AND status='RUNNING' AND attempt_no=1`, model.UUIDToBytes(projectID)).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL.QueryRowContext(ctx, `SELECT count(*) FROM lab_task_attempts a JOIN lab_tasks t ON t.id=a.task_id WHERE t.project_id=?`, model.UUIDToBytes(projectID)).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if running != 100 || attempts != 100 {
		t.Fatalf("running=%d attempts=%d", running, attempts)
	}

	rollbackTask := insertClaimTasks(t, ctx, db.SQL, projectID, 1)[0]
	setupWorker := uuid.New()
	if _, err := db.SQL.ExecContext(ctx, `INSERT INTO lab_task_attempts(task_id,attempt_no,worker_id,started_at,created_at) VALUES(?,1,?,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))`, model.UUIDToBytes(rollbackTask), model.UUIDToBytes(setupWorker)); err != nil {
		t.Fatal(err)
	}
	if _, err := claimer.Claim(ctx, scheduler.ClaimRequest{WorkerID: uuid.New(), Limit: 1, LeaseDuration: time.Minute}); err == nil {
		t.Fatal("claim succeeded despite duplicate attempt")
	}
	var status model.TaskStatus
	var attemptNo int
	if err := db.SQL.QueryRowContext(ctx, `SELECT status,attempt_no FROM lab_tasks WHERE id=?`, model.UUIDToBytes(rollbackTask)).Scan(&status, &attemptNo); err != nil {
		t.Fatal(err)
	}
	if status != model.TaskPending || attemptNo != 0 {
		t.Fatalf("failed claim was not rolled back: status=%s attempt=%d", status, attemptNo)
	}
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer timeoutCancel()
	<-timeoutCtx.Done()
	if _, err := claimer.Claim(timeoutCtx, scheduler.ClaimRequest{WorkerID: uuid.New(), Limit: 1, LeaseDuration: time.Minute}); err == nil {
		t.Fatal("claim ignored expired context")
	}
}

func insertClaimTasks(t *testing.T, ctx context.Context, db *sql.DB, projectID uuid.UUID, count int) []uuid.UUID {
	t.Helper()
	now := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	ids := make([]uuid.UUID, count)
	for i := range ids {
		ids[i] = uuid.New()
		if _, err := db.ExecContext(ctx, `INSERT INTO lab_tasks(id,project_id,status,priority,available_at,payload,created_at,updated_at) VALUES(?,?,'PENDING',?,?,JSON_OBJECT('index',?),?,?)`, model.UUIDToBytes(ids[i]), model.UUIDToBytes(projectID), i%10, now, i, now, now); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}
