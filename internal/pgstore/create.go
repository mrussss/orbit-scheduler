package pgstore

import (
	"context"
	"errors"

	"github.com/mrussss/orbit-scheduler/internal/command"
)

func (s *Store) CreateTask(context.Context, command.CreateTask) (command.CreatedTask, error) {
	return command.CreatedTask{}, errors.New("TODO: idempotent task creation")
}
func (s *Store) CreateJob(context.Context, command.CreateJob) (command.CreatedJob, error) {
	return command.CreatedJob{}, errors.New("TODO: idempotent job creation")
}
