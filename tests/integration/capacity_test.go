//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestFetchEnforcesWorkerAndProjectCapacity(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, migratedPostgres(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.New(pool, pgstore.Config{MaxFetchBatch: 20, RetryBase: time.Millisecond, RetryMax: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	projectID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES($1,'quota-project','ACTIVE',100,3,now(),now())`, projectID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO tasks(id,project_id,task_type,payload,payload_hash,status,priority,available_at,execution_timeout,max_attempts,created_at,updated_at) VALUES($1,$2,'quota','{}',decode(repeat('00',32),'hex'),'PENDING',$3,now(),interval '30 seconds',3,now(),now())`, uuid.New(), projectID, i); err != nil {
			t.Fatal(err)
		}
	}

	workers := make([]uuid.UUID, 8)
	for i := range workers {
		workers[i] = uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO worker_instances(id,worker_name,hostname,capacity,supported_task_types,running_tasks,draining,last_heartbeat_at,started_at,process_version,created_at,updated_at) VALUES($1,$2,'host',10,ARRAY['quota'],0,false,now(),now(),'test',now(),now())`, workers[i], workers[i].String()); err != nil {
			t.Fatal(err)
		}
	}

	type claimedTask struct {
		worker uuid.UUID
		task   scheduler.Assignment
	}
	claimed := make(chan claimedTask, 20)
	errors := make(chan error, len(workers))
	var wait sync.WaitGroup
	for _, workerID := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			assignments, fetchErr := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: workerID, Requested: 10, LeaseDuration: time.Minute})
			if fetchErr != nil {
				errors <- fetchErr
				return
			}
			for _, assignment := range assignments {
				claimed <- claimedTask{worker: workerID, task: assignment}
			}
		}()
	}
	wait.Wait()
	close(claimed)
	close(errors)
	for fetchErr := range errors {
		t.Fatal(fetchErr)
	}
	all := make([]claimedTask, 0, 3)
	for item := range claimed {
		all = append(all, item)
	}
	if len(all) != 3 {
		t.Fatalf("project quota claimed=%d, want 3", len(all))
	}

	winner := all[0].worker
	if err := store.HeartbeatWorker(ctx, winner, 0, false); err != nil {
		t.Fatal(err)
	}
	more, err := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: winner, Requested: 10, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(more) != 0 {
		t.Fatalf("heartbeat reopened scheduler capacity: claimed=%d", len(more))
	}
	var authoritative, reported int
	if err := pool.QueryRow(ctx, `SELECT running_tasks,reported_running_tasks FROM worker_instances WHERE id=$1`, winner).Scan(&authoritative, &reported); err != nil {
		t.Fatal(err)
	}
	winnerClaims := 0
	for _, item := range all {
		if item.worker == winner {
			winnerClaims++
		}
	}
	if authoritative != winnerClaims || reported != 0 {
		t.Fatalf("worker counts authoritative=%d reported=%d claims=%d", authoritative, reported, winnerClaims)
	}

	completed := all[0]
	now := time.Now().UTC()
	result := []byte(`{"ok":true}`)
	if _, err := store.ReportResult(ctx, scheduler.ReportRequest{TaskID: completed.task.TaskID, WorkerInstanceID: completed.worker, AttemptNo: completed.task.AttemptNo, Outcome: domain.OutcomeSucceeded, Result: result, ResultHash: domain.HashBytes(result), ExecutionStartedAt: now.Add(-time.Second), ExecutionFinishedAt: now}); err != nil {
		t.Fatal(err)
	}
	replacement, err := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: workers[len(workers)-1], Requested: 10, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(replacement) != 1 {
		t.Fatalf("released project slot claimed=%d, want 1", len(replacement))
	}
	var projectRunning int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE project_id=$1 AND status='RUNNING'`, projectID).Scan(&projectRunning); err != nil {
		t.Fatal(err)
	}
	if projectRunning != 3 {
		t.Fatalf("project running=%d, want 3", projectRunning)
	}
}
