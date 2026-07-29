package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/model"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestMySQLIsolationLevelsAndCurrentRead(t *testing.T) {
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

	tests := []struct {
		name      string
		isolation sql.IsolationLevel
		want      string
	}{
		{name: "read_committed_sees_new_commit", isolation: sql.LevelReadCommitted, want: "updated"},
		{name: "repeatable_read_keeps_snapshot", isolation: sql.LevelRepeatableRead, want: "initial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := insertIsolationProject(t, ctx, db.SQL, tc.name)
			first, second, current := coordinatedReads(t, ctx, db.SQL, id, tc.isolation)
			if first != "initial" || second != tc.want {
				t.Fatalf("first=%q second=%q want=%q", first, second, tc.want)
			}
			if tc.isolation == sql.LevelRepeatableRead && current != "updated" {
				t.Fatalf("FOR UPDATE current read=%q want=updated", current)
			}
		})
	}
}

func coordinatedReads(t *testing.T, ctx context.Context, db *sql.DB, id uuid.UUID, isolation sql.IsolationLevel) (string, string, string) {
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
	txA, err := connA.BeginTx(ctx, &sql.TxOptions{Isolation: isolation})
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback()
	var first string
	if err := txA.QueryRowContext(ctx, `SELECT name FROM lab_projects WHERE id=?`, model.UUIDToBytes(id)).Scan(&first); err != nil {
		t.Fatal(err)
	}
	aHasRead := make(chan struct{})
	bHasCommitted := make(chan error, 1)
	go func() {
		<-aHasRead
		txB, err := connB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err == nil {
			_, err = txB.ExecContext(ctx, `UPDATE lab_projects SET name='updated' WHERE id=?`, model.UUIDToBytes(id))
		}
		if err == nil {
			err = txB.Commit()
		} else if txB != nil {
			_ = txB.Rollback()
		}
		bHasCommitted <- err
	}()
	close(aHasRead)
	if err := <-bHasCommitted; err != nil {
		t.Fatal(err)
	}
	var second string
	if err := txA.QueryRowContext(ctx, `SELECT name FROM lab_projects WHERE id=?`, model.UUIDToBytes(id)).Scan(&second); err != nil {
		t.Fatal(err)
	}
	current := second
	if isolation == sql.LevelRepeatableRead {
		if err := txA.QueryRowContext(ctx, `SELECT name FROM lab_projects WHERE id=? FOR UPDATE`, model.UUIDToBytes(id)).Scan(&current); err != nil {
			t.Fatal(err)
		}
	}
	if err := txA.Commit(); err != nil {
		t.Fatal(err)
	}
	return first, second, current
}

func insertIsolationProject(t *testing.T, ctx context.Context, db *sql.DB, suffix string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.ExecContext(ctx, `INSERT INTO lab_projects(id,name,status,created_at,updated_at) VALUES(?, 'initial', 'ACTIVE', ?, ?)`, model.UUIDToBytes(id), now, now); err != nil {
		t.Fatal(fmt.Errorf("insert %s project: %w", suffix, err))
	}
	return id
}
