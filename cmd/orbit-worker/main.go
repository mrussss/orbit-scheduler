package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/config"
	"github.com/mrussss/orbit-scheduler/internal/executor"
	"github.com/mrussss/orbit-scheduler/internal/grpcclient"
	"github.com/mrussss/orbit-scheduler/internal/observability"
	"github.com/mrussss/orbit-scheduler/internal/platform"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/mrussss/orbit-scheduler/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("orbit-worker stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	logger, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	signalCtx, stopSignals := platform.SignalContext()
	defer stopSignals()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := grpcclient.Dial(dialCtx, cfg.GRPCAddr)
	cancelDial()
	if err != nil {
		return err
	}
	registry := executor.NewRegistry()
	registeredTypes := make([]string, 0, len(cfg.Worker.TaskTypes))
	for _, taskType := range cfg.Worker.TaskTypes {
		switch taskType {
		case "mock":
			mockExecutor := &executor.Mock{CrashHook: func(task scheduler.Assignment) {
				logger.Error("mock executor requested process crash", "task_id", task.TaskID, "attempt_no", task.AttemptNo)
				os.Exit(70)
			}}
			if err := registry.Register("mock", mockExecutor); err != nil {
				return err
			}
			registeredTypes = append(registeredTypes, "mock")
		case "http":
			httpExecutor, err := executor.NewHTTP(executor.HTTPConfig{AllowedHosts: cfg.HTTPExecutor.AllowedHosts, RequestTimeout: cfg.HTTPExecutor.RequestTimeout, DialTimeout: 3 * time.Second, TLSHandshakeTimeout: 3 * time.Second, MaxRequestBytes: cfg.HTTPExecutor.MaxRequestBytes, MaxResponseBytes: cfg.HTTPExecutor.MaxResponseBytes, MaxRedirects: cfg.HTTPExecutor.MaxRedirects})
			if err != nil {
				return err
			}
			if err := registry.Register("http", httpExecutor); err != nil {
				return err
			}
			registeredTypes = append(registeredTypes, "http")
		default:
			return fmt.Errorf("unsupported worker task type %q", taskType)
		}
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	instanceID := uuid.New()
	runtime, err := worker.NewRuntime(client, registry, logger, worker.Config{Registration: worker.Registration{WorkerName: cfg.Worker.Name, InstanceID: instanceID, Hostname: hostname, Capacity: cfg.Worker.Capacity, TaskTypes: registeredTypes, ProcessVersion: "dev"}, LeaseDuration: cfg.Worker.LeaseDuration, RenewInterval: cfg.Worker.RenewInterval, FetchInterval: cfg.Worker.FetchInterval, HeartbeatInterval: cfg.Worker.HeartbeatInterval, RPCDeadline: 5 * time.Second, ReportRetryBase: 200 * time.Millisecond, ReportRetries: 3, GracePeriod: cfg.Worker.GracePeriod})
	if err != nil {
		return err
	}
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	defer cancelRuntime()
	if err := runtime.Start(runtimeCtx); err != nil {
		_ = client.Close()
		return err
	}
	logger.Info("orbit-worker started", "worker_name", cfg.Worker.Name, "instance_id", instanceID, "capacity", cfg.Worker.Capacity, "task_types", registeredTypes)
	<-signalCtx.Done()
	logger.Info("orbit-worker draining", "grace_period", cfg.Worker.GracePeriod)
	graceCtx, cancelGrace := context.WithTimeout(context.Background(), cfg.Worker.GracePeriod)
	defer cancelGrace()
	if err := runtime.GracefulShutdown(graceCtx); err != nil {
		return err
	}
	logger.Info("orbit-worker stopped", "instance_id", instanceID)
	return nil
}
