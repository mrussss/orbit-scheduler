package testkit

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/config"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Environment struct {
	Container      *tcmysql.MySQLContainer
	DSN            string
	Config         config.Config
	MigrationsPath string
}

func StartMySQL(t testing.TB) Environment {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	container, err := tcmysql.RunContainer(ctx,
		testcontainers.WithImage("mysql:8.0.46"),
		tcmysql.WithDatabase("orbit_lab"),
		tcmysql.WithUsername("orbit"),
		tcmysql.WithPassword("orbit"),
		testcontainers.WithWaitStrategy(wait.ForLog("port: 3306  MySQL Community Server").WithOccurrence(1).WithStartupTimeout(3*time.Minute)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), time.Minute)
		defer terminateCancel()
		if err := container.Terminate(terminateCtx); err != nil {
			t.Errorf("terminate mysql container: %v", err)
		}
	})
	dsn, err := container.ConnectionString(ctx, "parseTime=true", "loc=UTC", "multiStatements=true", "charset=utf8mb4", "collation=utf8mb4_0900_ai_ci")
	if err != nil {
		t.Fatal(err)
	}
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve mysql testkit path")
	}
	migrationsPath := filepath.Clean(filepath.Join(filepath.Dir(current), "..", "..", "..", "..", "migrations", "mysql8"))
	cfg := config.Config{DSN: dsn, MaxOpenConns: 8, MaxIdleConns: 4, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, TxTimeout: 5 * time.Second}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return Environment{Container: container, DSN: dsn, Config: cfg, MigrationsPath: migrationsPath}
}
