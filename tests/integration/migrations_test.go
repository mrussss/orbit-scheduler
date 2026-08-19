//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMigrationsFromEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	dsn := migratedPostgres(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND indexname IN ('tasks_scheduler_candidates_idx','tasks_expired_lease_idx','outbox_unpublished_idx')`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("expected core indexes, got %d", count)
	}
	var agentTable string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.agent_steps')::text`).Scan(&agentTable); err != nil || agentTable != "agent_steps" {
		t.Fatalf("agent_steps migration missing: table=%q err=%v", agentTable, err)
	}
	_, err = conn.Exec(ctx, `INSERT INTO projects(id,name,status,task_quota,max_concurrent_tasks,created_at,updated_at) VALUES(gen_random_uuid(),'bad','UNKNOWN',0,1,now(),now())`)
	if err == nil {
		t.Fatal("project status constraint accepted invalid value")
	}
}

func migratedPostgres(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("orbit"), tcpostgres.WithUsername("orbit"), tcpostgres.WithPassword("orbit"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test path")
	}
	migrations := filepath.Join(filepath.Dir(current), "..", "..", "migrations", "postgres")
	m, err := migrate.New(fmt.Sprintf("file://%s", migrations), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Up(); err != nil {
		t.Fatal(err)
	}
	return dsn
}
