package pgstore

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	MaxFetchBatch int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

type Store struct {
	pool     *pgxpool.Pool
	cfg      Config
	random   *rand.Rand
	randomMu sync.Mutex
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
