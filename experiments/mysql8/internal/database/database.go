package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mrussss/orbit-scheduler/experiments/mysql8/internal/config"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Database struct {
	SQL       *sql.DB
	GORM      *gorm.DB
	txTimeout time.Duration
}

func Open(ctx context.Context, cfg config.Config) (*Database, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	sqlDB, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	gormDB, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: sqlDB}), &gorm.Config{TranslateError: false})
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("open gorm mysql: %w", err)
	}
	return &Database{SQL: sqlDB, GORM: gormDB, txTimeout: cfg.TxTimeout}, nil
}

func (d *Database) Ping(ctx context.Context) error {
	if d == nil || d.SQL == nil {
		return errors.New("mysql database is not open")
	}
	return d.SQL.PingContext(ctx)
}

func (d *Database) WithinTx(ctx context.Context, options *sql.TxOptions, fn func(context.Context, *sql.Tx) error) error {
	if d == nil || d.SQL == nil || fn == nil {
		return errors.New("mysql transaction dependencies are required")
	}
	txCtx, cancel := context.WithTimeout(ctx, d.txTimeout)
	defer cancel()
	tx, err := d.SQL.BeginTx(txCtx, options)
	if err != nil {
		return fmt.Errorf("begin mysql transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(txCtx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit mysql transaction: %w", err)
	}
	return nil
}

func (d *Database) Close() error {
	if d == nil || d.SQL == nil {
		return nil
	}
	return d.SQL.Close()
}
