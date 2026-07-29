package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	TxTimeout       time.Duration
}

func Load() (Config, error) {
	cfg := Config{DSN: os.Getenv("MYSQL_LAB_DSN")}
	var err error
	if cfg.MaxOpenConns, err = intEnv("MYSQL_LAB_MAX_OPEN_CONNS", 10); err != nil {
		return Config{}, err
	}
	if cfg.MaxIdleConns, err = intEnv("MYSQL_LAB_MAX_IDLE_CONNS", 5); err != nil {
		return Config{}, err
	}
	if cfg.ConnMaxLifetime, err = durationEnv("MYSQL_LAB_CONN_MAX_LIFETIME", 30*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.ConnMaxIdleTime, err = durationEnv("MYSQL_LAB_CONN_MAX_IDLE_TIME", 5*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.TxTimeout, err = durationEnv("MYSQL_LAB_TX_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	var problems []error
	parsed, err := drivermysql.ParseDSN(c.DSN)
	if err != nil {
		problems = append(problems, fmt.Errorf("MYSQL_LAB_DSN: %w", err))
	} else {
		if !parsed.ParseTime {
			problems = append(problems, errors.New("MYSQL_LAB_DSN must set parseTime=true"))
		}
		if parsed.Loc == nil || parsed.Loc.String() != "UTC" || !hasExplicitUTC(c.DSN) {
			problems = append(problems, errors.New("MYSQL_LAB_DSN must set loc=UTC"))
		}
		if parsed.MultiStatements {
			problems = append(problems, errors.New("MYSQL_LAB_DSN must not enable multiStatements; migrations use a derived DSN"))
		}
	}
	if c.MaxOpenConns <= 0 || c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns {
		problems = append(problems, errors.New("invalid MySQL Lab connection pool limits"))
	}
	if c.ConnMaxLifetime <= 0 || c.ConnMaxIdleTime <= 0 || c.TxTimeout <= 0 {
		problems = append(problems, errors.New("MySQL Lab time limits must be positive"))
	}
	return errors.Join(problems...)
}

func (c Config) MigrationDSN() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	parsed, err := drivermysql.ParseDSN(c.DSN)
	if err != nil {
		return "", fmt.Errorf("MYSQL_LAB_DSN: %w", err)
	}
	parsed.MultiStatements = true
	return parsed.FormatDSN(), nil
}

func hasExplicitUTC(dsn string) bool {
	_, rawQuery, found := strings.Cut(dsn, "?")
	if !found {
		return false
	}
	values, err := url.ParseQuery(rawQuery)
	return err == nil && values.Get("loc") == "UTC"
}

func intEnv(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
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
