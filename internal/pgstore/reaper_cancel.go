package pgstore

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
)

func (s *Store) ReapExpired(context.Context, int) (scheduler.ReapResult, error) {
	return scheduler.ReapResult{}, errors.New("TODO: lease reaper")
}

func (s *Store) CancelTask(context.Context, uuid.UUID, uuid.UUID) error {
	return errors.New("TODO: conditional task cancellation")
}

func (s *Store) CancelJob(context.Context, uuid.UUID, uuid.UUID) error {
	return errors.New("TODO: idempotent job cancellation")
}
