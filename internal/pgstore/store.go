package pgstore

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

type Config struct {
	MaxFetchBatch int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

type Store struct {
	pool   *pgxpool.Pool
	cfg    Config
	random *rand.Rand
}

func New(pool *pgxpool.Pool, cfg Config) (*Store, error) {
	if pool == nil {
		return nil, errors.New("pgx pool is required")
	}
	if cfg.MaxFetchBatch <= 0 {
		return nil, errors.New("max fetch batch must be positive")
	}
	if cfg.RetryBase <= 0 || cfg.RetryMax < cfg.RetryBase {
		return nil, errors.New("invalid retry backoff")
	}
	return &Store{pool: pool, cfg: cfg, random: rand.New(rand.NewSource(time.Now().UnixNano()))}, nil
}

func (s *Store) FetchTasks(context.Context, scheduler.FetchRequest) ([]scheduler.Assignment, error) {
	return nil, errors.New("TODO: atomic task fetch")
}
