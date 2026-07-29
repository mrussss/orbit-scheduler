package tests

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	deadlockretry "github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/retry"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestDeadlockReproductionAndBoundedRetry(t *testing.T) {
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
	firstID := insertIsolationProject(t, ctx, db.SQL, "deadlock-first")
	secondID := insertIsolationProject(t, ctx, db.SQL, "deadlock-second")
	deadlockErr := reproduceDeadlock(t, ctx, db.SQL, firstID, secondID)
	if !deadlockretry.IsDeadlock(deadlockErr) {
		t.Fatalf("expected MySQL deadlock, got %v", deadlockErr)
	}

	var attempts int
	seenTransactions := map[*sql.Tx]struct{}{}
	retries, err := deadlockretry.WithDeadlockRetry(ctx, db.SQL, 3, time.Millisecond, func(txCtx context.Context, tx *sql.Tx) error {
		attempts++
		if _, duplicate := seenTransactions[tx]; duplicate {
			t.Fatal("retry reused transaction")
		}
		seenTransactions[tx] = struct{}{}
		if attempts <= 2 {
			return deadlockErr
		}
		_, err := tx.ExecContext(txCtx, `UPDATE lab_projects SET updated_at=UTC_TIMESTAMP(6) WHERE id=?`, model.UUIDToBytes(firstID))
		return err
	})
	if err != nil || retries != 2 || attempts != 3 {
		t.Fatalf("retries=%d attempts=%d err=%v", retries, attempts, err)
	}
	nonDeadlock := errors.New("validation failed")
	attempts = 0
	retries, err = deadlockretry.WithDeadlockRetry(ctx, db.SQL, 3, time.Millisecond, func(context.Context, *sql.Tx) error {
		attempts++
		return nonDeadlock
	})
	if !errors.Is(err, nonDeadlock) || retries != 0 || attempts != 1 {
		t.Fatalf("non-deadlock retries=%d attempts=%d err=%v", retries, attempts, err)
	}
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := deadlockretry.WithDeadlockRetry(canceled, db.SQL, 3, time.Second, func(context.Context, *sql.Tx) error { return deadlockErr }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry err=%v", err)
	}
}

func reproduceDeadlock(t *testing.T, ctx context.Context, db *sql.DB, firstID, secondID uuid.UUID) error {
	t.Helper()
	connA, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	connB, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Close()
	txA, err := connA.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txB, err := connB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txA.ExecContext(ctx, `UPDATE lab_projects SET updated_at=updated_at WHERE id=?`, model.UUIDToBytes(firstID)); err != nil {
		t.Fatal(err)
	}
	if _, err := txB.ExecContext(ctx, `UPDATE lab_projects SET updated_at=updated_at WHERE id=?`, model.UUIDToBytes(secondID)); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsCh := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	go func() {
		ready.Done()
		<-start
		_, err := txA.ExecContext(ctx, `UPDATE lab_projects SET updated_at=updated_at WHERE id=?`, model.UUIDToBytes(secondID))
		errorsCh <- err
	}()
	go func() {
		ready.Done()
		<-start
		_, err := txB.ExecContext(ctx, `UPDATE lab_projects SET updated_at=updated_at WHERE id=?`, model.UUIDToBytes(firstID))
		errorsCh <- err
	}()
	ready.Wait()
	close(start)
	firstErr, secondErr := <-errorsCh, <-errorsCh
	_ = txA.Rollback()
	_ = txB.Rollback()
	for _, candidate := range []error{firstErr, secondErr} {
		var mysqlErr *drivermysql.MySQLError
		if errors.As(candidate, &mysqlErr) && mysqlErr.Number == 1213 {
			return candidate
		}
	}
	t.Fatalf("deadlock not observed: first=%v second=%v", firstErr, secondErr)
	return nil
}
