package migration

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type Runner struct{ migrate *migrate.Migrate }

func New(dsn, migrationsPath string) (*Runner, error) {
	absolutePath, err := filepath.Abs(migrationsPath)
	if err != nil {
		return nil, fmt.Errorf("resolve mysql migrations: %w", err)
	}
	m, err := migrate.New("file://"+filepath.ToSlash(absolutePath), "mysql://"+dsn)
	if err != nil {
		return nil, fmt.Errorf("create mysql migrator: %w", err)
	}
	return &Runner{migrate: m}, nil
}

func (r *Runner) Up() (bool, error) {
	err := r.migrate.Up()
	if errors.Is(err, migrate.ErrNoChange) {
		return false, nil
	}
	return err == nil, err
}

func (r *Runner) Down() (bool, error) {
	err := r.migrate.Down()
	if errors.Is(err, migrate.ErrNoChange) || errors.Is(err, migrate.ErrNilVersion) {
		return false, nil
	}
	return err == nil, err
}

func (r *Runner) Version() (uint, bool, error) { return r.migrate.Version() }

func (r *Runner) Close() error {
	sourceErr, databaseErr := r.migrate.Close()
	return errors.Join(sourceErr, databaseErr)
}
