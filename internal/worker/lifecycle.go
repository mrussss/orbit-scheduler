package worker

import (
	"context"
	"time"
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
func (r *Runtime) GracefulShutdown(ctx context.Context) error {
	state := r.State()
	if state == StateStopped {
		return nil
	}
	if state == StateInitialized {
		r.state.Store(int32(StateStopped))
		return r.client.Close()
	}
	r.state.Store(int32(StateDraining))
	r.draining.Store(true)
	r.drainOnce.Do(func() { close(r.drainCh) })
	if err := r.SetDraining(ctx, true); err != nil {
		r.logger.Warn("failed to announce draining", "error", err)
	}
	<-r.fetchDone
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		r.state.Store(int32(StateStopping))
		r.cancelTasks()
		forceWindow := r.cfg.RPCDeadline * time.Duration(r.cfg.ReportRetries+2)
		if forceWindow < time.Second {
			forceWindow = time.Second
		}
		forceCtx, cancel := context.WithTimeout(context.Background(), forceWindow)
		select {
		case <-done:
		case <-forceCtx.Done():
		}
		cancel()
	}
	r.state.Store(int32(StateStopping))
	if r.cancel != nil {
		r.cancel()
	}
	r.loops.Wait()
	r.state.Store(int32(StateStopped))
	return r.client.Close()
}
func (r *Runtime) cancelTasks() {
	r.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(r.tasks))
	for _, cancel := range r.tasks {
		cancels = append(cancels, cancel)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
