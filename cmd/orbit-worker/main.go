package main

import (
	"log/slog"
	"os"

	"github.com/mrussss/orbit-scheduler/internal/config"
	"github.com/mrussss/orbit-scheduler/internal/observability"
	"github.com/mrussss/orbit-scheduler/internal/platform"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		slog.Error("invalid logger configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := platform.SignalContext()
	defer stop()
	logger.Info("starting orbit-worker", "name", cfg.Worker.Name, "capacity", cfg.Worker.Capacity)
	<-ctx.Done()
	logger.Info("orbit-worker stopped")
}
