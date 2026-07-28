package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type PostgreSQL struct {
	GORM *gorm.DB
	PGX  *pgxpool.Pool
}

func OpenPostgreSQL(ctx context.Context, cfg config.Config) (*PostgreSQL, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse pgx config: %w", err)
	}
	poolCfg.MaxConns, poolCfg.MinConns = cfg.PGX.MaxConns, cfg.PGX.MinConns
	poolCfg.MaxConnLifetime, poolCfg.MaxConnIdleTime = cfg.PGX.MaxLifetime, cfg.PGX.MaxIdleTime
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("open pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	gormDB, err := gorm.Open(postgres.Open(cfg.DatabaseURL), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), NowFunc: func() time.Time { return time.Now().UTC() }})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("open gorm: %w", err)
	}
	sqlDB, err := gormDB.DB()
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("get gorm pool: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.GORM.MaxOpen)
	sqlDB.SetMaxIdleConns(cfg.GORM.MaxIdle)
	sqlDB.SetConnMaxLifetime(cfg.GORM.MaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.GORM.MaxIdleTime)
	return &PostgreSQL{GORM: gormDB, PGX: pool}, nil
}

func (p *PostgreSQL) Close() error {
	p.PGX.Close()
	sqlDB, err := p.GORM.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
