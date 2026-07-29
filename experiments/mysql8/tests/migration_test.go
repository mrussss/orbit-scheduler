package tests

import (
	"context"
	"testing"
	"time"

	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/database"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/migration"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/testkit"
)

func TestMigrationsUpRepeatAndDownFromEmptyMySQL(t *testing.T) {
	environment := testkit.StartMySQL(t)
	runner, err := migration.New(environment.MigrationDSN, environment.MigrationsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	changed, err := runner.Up()
	if err != nil || !changed {
		t.Fatalf("first up changed=%v err=%v", changed, err)
	}
	version, dirty, err := runner.Version()
	if err != nil || version != 4 || dirty {
		t.Fatalf("version=%d dirty=%v err=%v", version, dirty, err)
	}
	changed, err = runner.Up()
	if err != nil || changed {
		t.Fatalf("repeat up changed=%v err=%v", changed, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := database.Open(ctx, environment.Config)
	if err != nil {
		t.Fatal(err)
	}
	var tables int
	if err := db.SQL.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN ('lab_projects','lab_tasks','lab_task_attempts') AND engine='InnoDB'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 3 {
		t.Fatalf("InnoDB lab tables=%d", tables)
	}
	var indexes int
	if err := db.SQL.QueryRowContext(ctx, `SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='lab_tasks' AND index_name IN ('uk_lab_task_idempotency','idx_lab_task_page','idx_lab_task_fetch','idx_lab_task_lease')`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 4 {
		t.Fatalf("task indexes=%d", indexes)
	}
	var covering int
	if err := db.SQL.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='lab_tasks' AND index_name='idx_lab_task_page_cover'`).Scan(&covering); err != nil || covering != 5 {
		t.Fatalf("covering index columns=%d err=%v", covering, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	changed, err = runner.Down()
	if err != nil || !changed {
		t.Fatalf("down changed=%v err=%v", changed, err)
	}
	changed, err = runner.Down()
	if err != nil || changed {
		t.Fatalf("repeat down changed=%v err=%v", changed, err)
	}
}
