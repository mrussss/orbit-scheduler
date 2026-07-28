package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/mrussss/orbit-scheduler/internal/api"
	"github.com/mrussss/orbit-scheduler/internal/config"
	"github.com/mrussss/orbit-scheduler/internal/observability"
	"github.com/mrussss/orbit-scheduler/internal/platform"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if err := run(); err != nil {
		slog.Error("orbit-server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	ctx, stop := platform.SignalContext()
	defer stop()
	logger.Info("starting orbit-server", "http_addr", cfg.HTTPAddr, "grpc_addr", cfg.GRPCAddr)
	metrics := &http.Server{Addr: cfg.MetricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: cfg.HTTP.RequestTimeout}
	go func() {
		if err := platform.ServeHTTP(ctx, logger, metrics); err != nil {
			logger.Error("metrics server stopped", "error", err)
			stop()
		}
	}()
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: api.HealthRouter(nil), ReadHeaderTimeout: cfg.HTTP.RequestTimeout}
	return platform.ServeHTTP(ctx, logger, server)
}
