package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	workerv1 "github.com/mrussss/orbit-scheduler/gen/orbit/worker/v1"
	"github.com/mrussss/orbit-scheduler/internal/api"
	"github.com/mrussss/orbit-scheduler/internal/auth"
	"github.com/mrussss/orbit-scheduler/internal/business"
	"github.com/mrussss/orbit-scheduler/internal/config"
	"github.com/mrussss/orbit-scheduler/internal/database"
	"github.com/mrussss/orbit-scheduler/internal/domain"
	"github.com/mrussss/orbit-scheduler/internal/gormrepo"
	"github.com/mrussss/orbit-scheduler/internal/grpcservice"
	"github.com/mrussss/orbit-scheduler/internal/observability"
	"github.com/mrussss/orbit-scheduler/internal/pgstore"
	"github.com/mrussss/orbit-scheduler/internal/platform"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
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
	signalCtx, stop := platform.SignalContext()
	defer stop()
	connectCtx, cancelConnect := context.WithTimeout(signalCtx, 10*time.Second)
	db, err := database.OpenPostgreSQL(connectCtx, cfg)
	cancelConnect()
	if err != nil {
		return err
	}
	defer db.Close()
	sqlPool, err := db.GORM.DB()
	if err != nil {
		return err
	}
	if err := observability.RegisterDatabaseMetrics(prometheus.DefaultRegisterer, sqlPool, db.PGX); err != nil {
		return err
	}
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
	schedulerMetrics, err := observability.NewSchedulerMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	workerService, err := grpcservice.New(schedulerStore, schedulerMetrics)
	if err != nil {
		return err
	}
	logger.Info("starting orbit-server", "http_addr", cfg.HTTPAddr, "grpc_addr", cfg.GRPCAddr)
	metrics := &http.Server{Addr: cfg.MetricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
	router := api.NewRouter(logger, service, db.PGX, api.RouterConfig{MaxBodyBytes: cfg.HTTP.MaxBodyBytes, RequestTimeout: cfg.HTTP.RequestTimeout, AllowedOrigins: []string{"http://localhost:3000"}, CursorSecret: cfg.TokenPepper})
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: cfg.HTTP.RequestTimeout + time.Second, WriteTimeout: cfg.HTTP.RequestTimeout + time.Second, IdleTimeout: 60 * time.Second}

	group, ctx := errgroup.WithContext(signalCtx)
	group.Go(func() error { service.RunTokenTouches(ctx); return nil })
	group.Go(func() error { runReaper(ctx, logger, schedulerStore, schedulerMetrics); return nil })
	group.Go(func() error { runTaskStatusMetrics(ctx, logger, db.PGX, schedulerMetrics); return nil })
	group.Go(func() error { return serveGRPC(ctx, cfg.GRPCAddr, workerService) })
	group.Go(func() error { return serveHTTPComponent(ctx, logger, metrics, "metrics") })
	group.Go(func() error { return serveHTTPComponent(ctx, logger, server, "api") })
	return group.Wait()
}

func serveGRPC(ctx context.Context, address string, service workerv1.WorkerServiceServer) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(domain.MaxGRPCMessageBytes), grpc.MaxSendMsgSize(domain.MaxGRPCMessageBytes))
	workerv1.RegisterWorkerServiceServer(server, service)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	select {
	case err := <-errCh:
		if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		if err == nil {
			return errors.New("grpc server stopped unexpectedly")
		}
		return fmt.Errorf("serve grpc: %w", err)
	case <-ctx.Done():
		done := make(chan struct{})
		go func() { server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
		return nil
	}
}

func serveHTTPComponent(ctx context.Context, logger *slog.Logger, server *http.Server, name string) error {
	err := platform.ServeHTTP(ctx, logger, server)
	if err != nil {
		return fmt.Errorf("serve %s: %w", name, err)
	}
	if ctx.Err() == nil {
		return fmt.Errorf("%s server stopped unexpectedly", name)
	}
	return nil
}

type reaper interface {
	ReapExpired(context.Context, int) (scheduler.ReapResult, error)
}

type reaperMetrics interface {
	Reaper(scheduler.ReapResult, time.Duration)
}

func runReaper(ctx context.Context, logger *slog.Logger, store reaper, metrics reaperMetrics) {
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
			metrics.Reaper(result, time.Since(started))
			if result.Requeued+result.Failed+result.Canceled > 0 {
				logger.Info("reaped expired leases", "requeued", result.Requeued, "failed", result.Failed, "canceled", result.Canceled, "duration_ms", time.Since(started).Milliseconds())
			}
		}
	}
}

type taskStatusQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type taskStatusMetrics interface {
	SetTaskStatus(string, float64)
}

func runTaskStatusMetrics(ctx context.Context, logger *slog.Logger, pool taskStatusQuerier, metrics taskStatusMetrics) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	statuses := []string{"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED"}
	collect := func() {
		counts := map[string]float64{}
		rows, err := pool.Query(ctx, `SELECT status,count(*) FROM tasks GROUP BY status`)
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn("collect task status metrics failed", "error", err)
			}
			return
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			var count int64
			if err := rows.Scan(&status, &count); err != nil {
				logger.Warn("scan task status metrics failed", "error", err)
				return
			}
			counts[status] = float64(count)
		}
		if err := rows.Err(); err != nil && ctx.Err() == nil {
			logger.Warn("iterate task status metrics failed", "error", err)
			return
		}
		for _, status := range statuses {
			metrics.SetTaskStatus(status, counts[status])
		}
	}
	collect()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collect()
		}
	}
}
