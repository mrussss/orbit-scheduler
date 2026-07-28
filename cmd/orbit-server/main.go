package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	workerv1 "github.com/mrussss/orbit-scheduler/gen/orbit/worker/v1"
	"github.com/mrussss/orbit-scheduler/internal/api"
	"github.com/mrussss/orbit-scheduler/internal/auth"
	"github.com/mrussss/orbit-scheduler/internal/business"
	"github.com/mrussss/orbit-scheduler/internal/config"
	"github.com/mrussss/orbit-scheduler/internal/database"
	"github.com/mrussss/orbit-scheduler/internal/gormrepo"
	"github.com/mrussss/orbit-scheduler/internal/grpcservice"
	"github.com/mrussss/orbit-scheduler/internal/observability"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/platform"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
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
	connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
	db, err := database.OpenPostgreSQL(connectCtx, cfg)
	cancelConnect()
	if err != nil {
		return err
	}
	defer db.Close()
	queries, err := gormrepo.New(db.GORM)
	if err != nil {
		return err
	}
	schedulerStore, err := pgstore.New(db.PGX, pgstore.Config{MaxFetchBatch: 100, RetryBase: time.Second, RetryMax: 5 * time.Minute})
	if err != nil {
		return err
	}
	tokenCodec, err := auth.NewTokenCodec(cfg.TokenPepper)
	if err != nil {
		return err
	}
	service, err := business.New(queries, schedulerStore, tokenCodec, cfg.AdminToken)
	if err != nil {
		return err
	}
	go service.RunTokenTouches(ctx)
	go runReaper(ctx, logger, schedulerStore)
	workerService, err := grpcservice.New(schedulerStore)
	if err != nil {
		return err
	}
	if err := serveGRPC(ctx, logger, cfg.GRPCAddr, workerService); err != nil {
		return err
	}
	logger.Info("starting orbit-server", "http_addr", cfg.HTTPAddr, "grpc_addr", cfg.GRPCAddr)
	metrics := &http.Server{Addr: cfg.MetricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := platform.ServeHTTP(ctx, logger, metrics); err != nil {
			logger.Error("metrics server stopped", "error", err)
			stop()
		}
	}()
	router := api.NewRouter(logger, service, db.PGX, api.RouterConfig{MaxBodyBytes: cfg.HTTP.MaxBodyBytes, RequestTimeout: cfg.HTTP.RequestTimeout, AllowedOrigins: []string{"http://localhost:3000"}, CursorSecret: cfg.TokenPepper})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: cfg.HTTP.RequestTimeout + time.Second, WriteTimeout: cfg.HTTP.RequestTimeout + time.Second, IdleTimeout: 60 * time.Second}
	return platform.ServeHTTP(ctx, logger, server)
}

func serveGRPC(ctx context.Context, logger *slog.Logger, address string, service workerv1.WorkerServiceServer) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	workerv1.RegisterWorkerServiceServer(server, service)
	go func() {
		if err := server.Serve(listener); err != nil {
			if ctx.Err() == nil {
				logger.Error("grpc server stopped", "error", err)
			}
		}
	}()
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() { server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
	}()
	return nil
}

type reaper interface {
	ReapExpired(context.Context, int) (scheduler.ReapResult, error)
}

func runReaper(ctx context.Context, logger *slog.Logger, store reaper) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			started := time.Now()
			result, err := store.ReapExpired(ctx, 100)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("lease reaper failed", "error", err)
				}
				continue
			}
			if result.Requeued+result.Failed+result.Canceled > 0 {
				logger.Info("reaped expired leases", "requeued", result.Requeued, "failed", result.Failed, "canceled", result.Canceled, "duration_ms", time.Since(started).Milliseconds())
			}
		}
	}
}
