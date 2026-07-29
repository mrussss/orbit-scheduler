package testkit

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/config"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Environment struct {
	Container      *mysql.MySQLContainer
	DSN            string
	MigrationDSN   string
	Config         config.Config
	MigrationsPath string
}

func StartMySQL(t testing.TB) Environment {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	container, err := mysql.Run(ctx, "mysql:8.0.46",
		mysql.WithDatabase("orbit_lab"),
		mysql.WithUsername("orbit"),
		mysql.WithPassword("orbit"),
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForListeningPort(nat.Port("3306/tcp")),
			wait.ForSQL(nat.Port("3306/tcp"), "mysql", func(host string, port nat.Port) string {
				return fmt.Sprintf("orbit:orbit@tcp(%s:%s)/orbit_lab?parseTime=true&loc=UTC", host, port.Port())
			}),
		).WithStartupTimeout(3*time.Minute)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container, testcontainers.StopTimeout(30*time.Second)); err != nil {
			t.Errorf("terminate mysql container: %v", err)
		}
	})
	dsn, err := container.ConnectionString(ctx, "parseTime=true", "loc=UTC", "charset=utf8mb4", "collation=utf8mb4_0900_ai_ci")
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
	migrationDSN, err := cfg.MigrationDSN()
	if err != nil {
		t.Fatal(err)
	}
	return Environment{Container: container, DSN: dsn, MigrationDSN: migrationDSN, Config: cfg, MigrationsPath: migrationsPath}
}
