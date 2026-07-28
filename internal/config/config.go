package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv       string
	LogLevel     string
	HTTPAddr     string
	GRPCAddr     string
	MetricsAddr  string
	DatabaseURL  string
	TokenPepper  string
	AdminToken   string
	KafkaBrokers []string
	TaskTopic    string
	TaskDLQTopic string
	GORM         SQLPool
	PGX          PGXPool
	HTTP         HTTP
	Worker       Worker
}

type SQLPool struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

type PGXPool struct {
	MaxConns    int32
	MinConns    int32
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

type HTTP struct {
	RequestTimeout time.Duration
	MaxBodyBytes   int64
}

type Worker struct {
	Name              string
	Capacity          int
	TaskTypes         []string
	GracePeriod       time.Duration
	FetchInterval     time.Duration
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	RenewInterval     time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		AppEnv:       env("APP_ENV", "development"),
		LogLevel:     env("LOG_LEVEL", "info"),
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		GRPCAddr:     env("GRPC_ADDR", ":9090"),
		MetricsAddr:  env("METRICS_ADDR", ":9091"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		TokenPepper:  os.Getenv("TOKEN_PEPPER"),
		AdminToken:   os.Getenv("ADMIN_TOKEN"),
		KafkaBrokers: split(env("KAFKA_BROKERS", "localhost:9092")),
		TaskTopic:    env("KAFKA_TASK_EVENTS_TOPIC", "orbit.task-events.v1"),
		TaskDLQTopic: env("KAFKA_TASK_EVENTS_DLQ_TOPIC", "orbit.task-events.dlq.v1"),
	}
	var err error
	if cfg.GORM.MaxOpen, err = intEnv("GORM_MAX_OPEN_CONNS", 10); err != nil {
		return Config{}, err
	}
	if cfg.GORM.MaxIdle, err = intEnv("GORM_MAX_IDLE_CONNS", 5); err != nil {
		return Config{}, err
	}
	if cfg.GORM.MaxLifetime, err = durationEnv("DB_CONN_MAX_LIFETIME", 30*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.GORM.MaxIdleTime, err = durationEnv("DB_CONN_MAX_IDLE_TIME", 5*time.Minute); err != nil {
		return Config{}, err
	}
	maxConns, err := intEnv("PGX_MAX_CONNS", 20)
	if err != nil {
		return Config{}, err
	}
	minConns, err := intEnv("PGX_MIN_CONNS", 2)
	if err != nil {
		return Config{}, err
	}
	cfg.PGX.MaxConns, cfg.PGX.MinConns = int32(maxConns), int32(minConns)
	cfg.PGX.MaxLifetime, cfg.PGX.MaxIdleTime = cfg.GORM.MaxLifetime, cfg.GORM.MaxIdleTime
	if cfg.HTTP.RequestTimeout, err = durationEnv("HTTP_REQUEST_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	body, err := intEnv("HTTP_MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return Config{}, err
	}
	cfg.HTTP.MaxBodyBytes = int64(body)
	cfg.Worker.Name = env("WORKER_NAME", "orbit-worker")
	if cfg.Worker.Capacity, err = intEnv("WORKER_CAPACITY", 4); err != nil {
		return Config{}, err
	}
	cfg.Worker.TaskTypes = split(env("WORKER_TASK_TYPES", "mock,http"))
	if cfg.Worker.GracePeriod, err = durationEnv("WORKER_GRACE_PERIOD", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Worker.FetchInterval, err = durationEnv("WORKER_FETCH_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.Worker.HeartbeatInterval, err = durationEnv("WORKER_HEARTBEAT_INTERVAL", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Worker.LeaseDuration, err = durationEnv("WORKER_LEASE_DURATION", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Worker.RenewInterval, err = durationEnv("WORKER_RENEW_INTERVAL", 10*time.Second); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if len(c.TokenPepper) < 32 {
		errs = append(errs, errors.New("TOKEN_PEPPER must contain at least 32 characters"))
	}
	if len(c.AdminToken) < 32 {
		errs = append(errs, errors.New("ADMIN_TOKEN must contain at least 32 characters"))
	}
	if c.GORM.MaxOpen <= 0 || c.GORM.MaxIdle < 0 || c.GORM.MaxIdle > c.GORM.MaxOpen {
		errs = append(errs, errors.New("invalid GORM connection pool limits"))
	}
	if c.PGX.MaxConns <= 0 || c.PGX.MinConns < 0 || c.PGX.MinConns > c.PGX.MaxConns {
		errs = append(errs, errors.New("invalid PGX connection pool limits"))
	}
	if c.Worker.Capacity <= 0 || c.Worker.Capacity > 1024 {
		errs = append(errs, errors.New("WORKER_CAPACITY must be between 1 and 1024"))
	}
	if c.HTTP.RequestTimeout <= 0 || c.HTTP.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("HTTP timeout and body limit must be positive"))
	}
	if c.Worker.RenewInterval <= 0 || c.Worker.RenewInterval >= c.Worker.LeaseDuration {
		errs = append(errs, errors.New("worker renew interval must be shorter than lease duration"))
	}
	return errors.Join(errs...)
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
func split(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
func intEnv(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}
