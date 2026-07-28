package worker

import (
	"context"
	"errors"
)

type State int32

const (
	StateInitialized State = iota
	StateRunning
	StateDraining
	StateStopping
	StateStopped
)

func (r *Runtime) State() State { return State(r.state.Load()) }
func (r *Runtime) GracefulShutdown(context.Context) error {
	return errors.New("TODO: graceful worker shutdown lifecycle")
}
