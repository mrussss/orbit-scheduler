package retry

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

const mysqlDeadlock = 1213

func WithDeadlockRetry(
	ctx context.Context,
	db *sql.DB,
	maxRetries int,
	baseDelay time.Duration,
	fn func(context.Context, *sql.Tx) error,
) (int, error) {
	if db == nil || fn == nil || maxRetries < 0 || baseDelay <= 0 {
		return 0, errors.New("invalid deadlock retry configuration")
	}
	for retries := 0; ; retries++ {
		if err := ctx.Err(); err != nil {
			return retries, err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return retries, fmt.Errorf("begin retry transaction: %w", err)
		}
		err = fn(ctx, tx)
		if err == nil {
			err = tx.Commit()
			if err != nil {
				_ = tx.Rollback()
			}
		} else {
			_ = tx.Rollback()
		}
		if err == nil {
			return retries, nil
		}
		if !IsDeadlock(err) {
			return retries, err
		}
		if retries >= maxRetries {
			return retries, fmt.Errorf("deadlock retry limit reached after %d retries: %w", retries, err)
		}
		delay := exponentialDelay(baseDelay, retries)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return retries + 1, ctx.Err()
		case <-timer.C:
		}
	}
}

func IsDeadlock(err error) bool {
	var mysqlErr *drivermysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlDeadlock
}

func exponentialDelay(base time.Duration, retry int) time.Duration {
	shift := min(retry, 10)
	ceiling := base * time.Duration(1<<shift)
	if ceiling <= 1 {
		return ceiling
	}
	jitter, err := rand.Int(rand.Reader, big.NewInt(int64(ceiling)))
	if err != nil {
		return ceiling
	}
	return ceiling + time.Duration(jitter.Int64())
}
