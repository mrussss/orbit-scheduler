package pgstore

import (
	"context"
	"errors"

	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func (s *Store) RenewLease(context.Context, scheduler.RenewRequest) (scheduler.RenewResult, error) {
	return scheduler.RenewResult{}, errors.New("TODO: fenced lease renewal")
}

func (s *Store) ReportResult(context.Context, scheduler.ReportRequest) (scheduler.ReportResult, error) {
	return scheduler.ReportResult{}, errors.New("TODO: idempotent result transaction")
}
