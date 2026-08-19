package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrussss/orbit-scheduler/internal/config"
	"github.com/mrussss/orbit-scheduler/internal/executor"
	agentexecutor "github.com/mrussss/orbit-scheduler/internal/executor/agent"
	llmexecutor "github.com/mrussss/orbit-scheduler/internal/executor/llm"
	"github.com/mrussss/orbit-scheduler/internal/grpcclient"
	"github.com/mrussss/orbit-scheduler/internal/observability"
	"github.com/mrussss/orbit-scheduler/internal/platform"
	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/mrussss/orbit-scheduler/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	defer func() { _ = client.Close() }()
	registry := executor.NewRegistry()
	instanceID := uuid.New()
	workerObserver, err := worker.NewPrometheusObserver(prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	var provider *llmexecutor.OpenAICompatible
	var llmObserver llmexecutor.Observer
	for _, taskType := range cfg.Worker.TaskTypes {
		if taskType != "llm" && taskType != "agent" {
			continue
		}
		provider, err = llmexecutor.NewOpenAICompatible(llmexecutor.OpenAICompatibleConfig{
			BaseURL: cfg.LLMExecutor.BaseURL, APIKey: cfg.LLMExecutor.APIKey,
			RequestTimeout: cfg.LLMExecutor.RequestTimeout, DialTimeout: cfg.LLMExecutor.DialTimeout,
			TLSHandshakeTimeout: cfg.LLMExecutor.TLSHandshakeTimeout, MaxResponseBytes: cfg.LLMExecutor.MaxResponseBytes,
			MaxIdleConnsPerHost: max(cfg.LLMExecutor.MaxConcurrency, cfg.AgentExecutor.MaxConcurrency), AllowHTTP: strings.EqualFold(cfg.AppEnv, "test"),
		})
		if err != nil {
			return err
		}
		defer provider.CloseIdleConnections()
		observer, observerErr := llmexecutor.NewPrometheusObserver(prometheus.DefaultRegisterer)
		if observerErr != nil {
			return observerErr
		}
		llmObserver = observer
		break
	}
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
		case "llm":
			costTable := make(map[string]llmexecutor.Cost, len(cfg.LLMExecutor.CostTable))
			for model, cost := range cfg.LLMExecutor.CostTable {
				costTable[model] = llmexecutor.Cost{PromptMicrounitsPerMillionTokens: cost.PromptMicrounitsPerMillionTokens, CompletionMicrounitsPerMillionTokens: cost.CompletionMicrounitsPerMillionTokens}
			}
			llm, err := llmexecutor.NewExecutor(llmexecutor.ExecutorConfig{
				ProviderName: cfg.LLMExecutor.Provider, Provider: provider, AllowedModels: cfg.LLMExecutor.AllowedModels,
				MaxPromptBytes: cfg.LLMExecutor.MaxPromptBytes, MaxOutputTokens: cfg.LLMExecutor.MaxOutputTokens,
				MaxConcurrency: cfg.LLMExecutor.MaxConcurrency, CostTable: costTable, Observer: llmObserver,
			})
			if err != nil {
				return err
			}
			if err := registry.Register("llm", llm); err != nil {
				return err
			}
			registeredTypes = append(registeredTypes, "llm")
		case "agent":
			toolbox, err := agentexecutor.NewToolbox(cfg.AgentExecutor.Repositories, agentexecutor.ToolLimits{MaxFileBytes: int64(cfg.AgentExecutor.MaxFileBytes), MaxResultBytes: cfg.AgentExecutor.MaxResultBytes, MaxMatches: cfg.AgentExecutor.MaxSearchMatches})
			if err != nil {
				return err
			}
			configuredCost := cfg.LLMExecutor.CostTable[cfg.AgentExecutor.Model]
			tracer := agentexecutor.TracerFunc(func(ctx context.Context, step agentexecutor.TraceStep) error {
				return client.RecordAgentStep(ctx, scheduler.RecordAgentStepRequest{TaskID: step.TaskID, WorkerInstanceID: instanceID, AttemptNo: step.AttemptNo, StepNo: step.StepNo, Kind: scheduler.AgentStepKind(step.Kind), ToolName: step.ToolName, InputSummary: step.InputSummary, OutputSummary: step.OutputSummary, Status: scheduler.AgentStepStatus(step.Status), StartedAt: step.StartedAt, FinishedAt: step.FinishedAt})
			})
			agent, err := agentexecutor.NewExecutor(agentexecutor.ExecutorConfig{Provider: provider, Toolbox: toolbox, Tracer: tracer, Repositories: cfg.AgentExecutor.Repositories, Model: cfg.AgentExecutor.Model, MaxIssueBytes: cfg.AgentExecutor.MaxIssueBytes, MaxOutputTokens: cfg.LLMExecutor.MaxOutputTokens, MaxModelSteps: cfg.AgentExecutor.MaxModelSteps, MaxToolCalls: cfg.AgentExecutor.MaxToolCalls, MaxConcurrency: cfg.AgentExecutor.MaxConcurrency, Cost: agentexecutor.Cost{PromptMicrounitsPerMillionTokens: configuredCost.PromptMicrounitsPerMillionTokens, CompletionMicrounitsPerMillionTokens: configuredCost.CompletionMicrounitsPerMillionTokens}})
			if err != nil {
				return err
			}
			if err := registry.Register("agent", agent); err != nil {
				return err
			}
			registeredTypes = append(registeredTypes, "agent")
		default:
			return fmt.Errorf("unsupported worker task type %q", taskType)
		}
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	runtime, err := worker.NewRuntime(client, registry, logger, worker.Config{Registration: worker.Registration{WorkerName: cfg.Worker.Name, InstanceID: instanceID, Hostname: hostname, Capacity: cfg.Worker.Capacity, TaskTypes: registeredTypes, ProcessVersion: "dev"}, LeaseDuration: cfg.Worker.LeaseDuration, RenewInterval: cfg.Worker.RenewInterval, FetchInterval: cfg.Worker.FetchInterval, HeartbeatInterval: cfg.Worker.HeartbeatInterval, RPCDeadline: 5 * time.Second, ReportRetryBase: 200 * time.Millisecond, ReportRetries: 3, GracePeriod: cfg.Worker.GracePeriod, Observer: workerObserver})
	if err != nil {
		return err
	}
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	defer cancelRuntime()
	if err := runtime.Start(runtimeCtx); err != nil {
		_ = client.Close()
		return err
	}
	metricsCtx, cancelMetrics := context.WithCancel(context.Background())
	defer cancelMetrics()
	metricsErr := make(chan error, 1)
	if cfg.Worker.MetricsAddr != "" {
		metricsServer := &http.Server{Addr: cfg.Worker.MetricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { metricsErr <- platform.ServeHTTP(metricsCtx, logger, metricsServer) }()
	}
	logger.Info("orbit-worker started", "worker_name", cfg.Worker.Name, "instance_id", instanceID, "capacity", cfg.Worker.Capacity, "task_types", registeredTypes)
	if cfg.Worker.MetricsAddr == "" {
		<-signalCtx.Done()
	} else {
		select {
		case <-signalCtx.Done():
		case err := <-metricsErr:
			stopCtx, cancelStop := context.WithTimeout(context.Background(), cfg.Worker.GracePeriod)
			_ = runtime.StopNow(stopCtx)
			cancelStop()
			if err == nil {
				err = errors.New("worker metrics server stopped unexpectedly")
			}
			return fmt.Errorf("serve worker metrics: %w", err)
		}
	}
	logger.Info("orbit-worker draining", "grace_period", cfg.Worker.GracePeriod)
	graceCtx, cancelGrace := context.WithTimeout(context.Background(), cfg.Worker.GracePeriod)
	defer cancelGrace()
	if err := runtime.GracefulShutdown(graceCtx); err != nil {
		return err
	}
	cancelMetrics()
	if cfg.Worker.MetricsAddr != "" {
		if err := <-metricsErr; err != nil {
			return fmt.Errorf("stop worker metrics: %w", err)
		}
	}
	logger.Info("orbit-worker stopped", "instance_id", instanceID)
	return nil
}
