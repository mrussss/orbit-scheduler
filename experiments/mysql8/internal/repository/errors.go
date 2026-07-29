package repository

import (
	"context"
	"errors"
	"fmt"

	drivermysql "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

var (
	ErrNotFound     = errors.New("mysql lab record not found")
	ErrDuplicate    = errors.New("mysql lab unique constraint conflict")
	ErrConstraint   = errors.New("mysql lab constraint violation")
	ErrInvalidState = errors.New("mysql lab invalid state transition")
	ErrInvalid      = errors.New("mysql lab invalid argument")
)

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	var mysqlErr *drivermysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062:
			return fmt.Errorf("%w: %s", ErrDuplicate, mysqlErr.Message)
		case 1451, 1452, 3819:
			return fmt.Errorf("%w: %s", ErrConstraint, mysqlErr.Message)
		}
	}
	return err
}
