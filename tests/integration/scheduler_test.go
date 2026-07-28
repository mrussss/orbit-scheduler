//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func TestAtomicFetchSkipsLocksAndRollsBackSideEffectFailure(t *testing.T) {
	ctx := context.Background()
	dsn := migratedPostgres(t)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := pgstore.New(pool, pgstore.Config{MaxFetchBatch: 20, RetryBase: time.Second, RetryMax: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	projectID := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES($1,'project','ACTIVE',1000,1000,now(),now())`, projectID)
	if err != nil {
		t.Fatal(err)
	}
	workers := make([]uuid.UUID, 10)
	for i := range workers {
		workers[i] = uuid.New()
		_, err = pool.Exec(ctx, `INSERT INTO worker_instances(id,worker_name,hostname,capacity,supported_task_types,running_tasks,draining,last_heartbeat_at,started_at,process_version,created_at,updated_at) VALUES($1,$2,'host',20,ARRAY['mock'],0,false,now(),now(),'test',now(),now())`, workers[i], fmt.Sprintf("worker-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		_, err = pool.Exec(ctx, `INSERT INTO tasks(id,project_id,task_type,payload,payload_hash,status,priority,available_at,execution_timeout,max_attempts,created_at,updated_at) VALUES($1,$2,'mock','{}',decode(repeat('00',32),'hex'),'PENDING',$3,now(),interval '30 seconds',3,now(),now())`, uuid.New(), projectID, i%3)
		if err != nil {
			t.Fatal(err)
		}
	}
	claimed := make(chan uuid.UUID, 100)
	errCh := make(chan error, len(workers))
	var wg sync.WaitGroup
	for _, workerID := range workers {
		wg.Add(1)
		go func(id uuid.UUID) {
			defer wg.Done()
			tasks, fetchErr := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: id, Requested: 20, LeaseDuration: 30 * time.Second})
			if fetchErr != nil {
				errCh <- fetchErr
				return
			}
			for _, task := range tasks {
				claimed <- task.TaskID
			}
		}(workerID)
	}
	wg.Wait()
	close(claimed)
	close(errCh)
	for fetchErr := range errCh {
		t.Fatal(fetchErr)
	}
	seen := map[uuid.UUID]struct{}{}
	for id := range claimed {
		if _, exists := seen[id]; exists {
			t.Fatalf("task %s claimed twice", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != 100 {
		t.Fatalf("claimed %d tasks", len(seen))
	}
	var running, attempts, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='RUNNING'`).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE event_type='TASK_STARTED'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if running != 100 || attempts != 100 || events != 100 {
		t.Fatalf("running=%d attempts=%d events=%d", running, attempts, events)
	}

	rollbackTask, rollbackWorker := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO worker_instances(id,worker_name,hostname,capacity,supported_task_types,running_tasks,draining,last_heartbeat_at,started_at,process_version,created_at,updated_at) VALUES($1,'rollback-worker','host',1,ARRAY['rollback'],0,false,now(),now(),'test',now(),now())`, rollbackWorker)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO tasks(id,project_id,task_type,payload,payload_hash,status,priority,available_at,execution_timeout,max_attempts,created_at,updated_at) VALUES($1,$2,'rollback','{}',decode(repeat('00',32),'hex'),'PENDING',0,now(),interval '30 seconds',3,now(),now())`, rollbackTask, projectID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO task_attempts(task_id,attempt_no,worker_name,worker_instance_id,started_at,created_at,updated_at) VALUES($1,1,'collision',$2,now(),now(),now())`, rollbackTask, rollbackWorker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FetchTasks(ctx, scheduler.FetchRequest{WorkerInstanceID: rollbackWorker, Requested: 1, LeaseDuration: time.Minute}); err == nil {
		t.Fatal("expected attempt insert collision")
	}
	var status string
	var attemptNo int
	if err := pool.QueryRow(ctx, `SELECT status,attempt_no FROM tasks WHERE id=$1`, rollbackTask).Scan(&status, &attemptNo); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || attemptNo != 0 {
		t.Fatalf("failed transaction leaked task state: %s attempt=%d", status, attemptNo)
	}
}
