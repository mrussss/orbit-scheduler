package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv       string
	LogLevel     string
	HTTPAddr     string
	GRPCAddr     string
	MetricsAddr  string
	DatabaseURL  string
	TokenPepper  string
	AdminToken   string
	KafkaBrokers []string
	TaskTopic    string
	TaskDLQTopic string
	GORM         SQLPool
	PGX          PGXPool
	HTTP         HTTP
	Worker       Worker
	HTTPExecutor HTTPExecutor
	LLMExecutor  LLMExecutor
}

type SQLPool struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

type PGXPool struct {
	MaxConns    int32
	MinConns    int32
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

type HTTP struct {
	RequestTimeout time.Duration
	MaxBodyBytes   int64
}

type Worker struct {
	Name              string
	MetricsAddr       string
	Capacity          int
	TaskTypes         []string
	GracePeriod       time.Duration
	FetchInterval     time.Duration
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	RenewInterval     time.Duration
}
type HTTPExecutor struct {
	AllowedHosts                      []string
	RequestTimeout                    time.Duration
	MaxRequestBytes, MaxResponseBytes int64
	MaxRedirects                      int
}

type LLMExecutor struct {
	Provider                         string
	BaseURL                          string
	APIKey                           string
	AllowedModels                    []string
	RequestTimeout                   time.Duration
	DialTimeout                      time.Duration
	TLSHandshakeTimeout              time.Duration
	MaxPromptBytes, MaxResponseBytes int64
	MaxOutputTokens, MaxConcurrency  int
	CostTable                        map[string]LLMCost
	LogContent, ToolCallingEnabled   bool
	MaxToolRounds                    int
}

type LLMCost struct {
	PromptMicrounitsPerMillionTokens     int64 `json:"prompt_microunits_per_million_tokens"`
	CompletionMicrounitsPerMillionTokens int64 `json:"completion_microunits_per_million_tokens"`
}

func load() (Config, error) {
	cfg := Config{
		AppEnv:       env("APP_ENV", "development"),
		LogLevel:     env("LOG_LEVEL", "info"),
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		GRPCAddr:     env("GRPC_ADDR", ":9090"),
		MetricsAddr:  env("METRICS_ADDR", ":9091"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		TokenPepper:  os.Getenv("TOKEN_PEPPER"),
		AdminToken:   os.Getenv("ADMIN_TOKEN"),
		KafkaBrokers: split(env("KAFKA_BROKERS", "localhost:9092")),
		TaskTopic:    env("KAFKA_TASK_EVENTS_TOPIC", "orbit.task-events.v1"),
		TaskDLQTopic: env("KAFKA_TASK_EVENTS_DLQ_TOPIC", "orbit.task-events.dlq.v1"),
	}
	var err error
	if cfg.GORM.MaxOpen, err = intEnv("GORM_MAX_OPEN_CONNS", 10); err != nil {
		return Config{}, err
	}
	if cfg.GORM.MaxIdle, err = intEnv("GORM_MAX_IDLE_CONNS", 5); err != nil {
		return Config{}, err
	}
	if cfg.GORM.MaxLifetime, err = durationEnv("DB_CONN_MAX_LIFETIME", 30*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.GORM.MaxIdleTime, err = durationEnv("DB_CONN_MAX_IDLE_TIME", 5*time.Minute); err != nil {
		return Config{}, err
	}
	maxConns, err := intEnv("PGX_MAX_CONNS", 20)
	if err != nil {
		return Config{}, err
	}
	minConns, err := intEnv("PGX_MIN_CONNS", 2)
	if err != nil {
		return Config{}, err
	}
	cfg.PGX.MaxConns, cfg.PGX.MinConns = int32(maxConns), int32(minConns)
	cfg.PGX.MaxLifetime, cfg.PGX.MaxIdleTime = cfg.GORM.MaxLifetime, cfg.GORM.MaxIdleTime
	if cfg.HTTP.RequestTimeout, err = durationEnv("HTTP_REQUEST_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	body, err := intEnv("HTTP_MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return Config{}, err
	}
	cfg.HTTP.MaxBodyBytes = int64(body)
	cfg.Worker.Name = env("WORKER_NAME", "orbit-worker")
	cfg.Worker.MetricsAddr = strings.TrimSpace(os.Getenv("WORKER_METRICS_ADDR"))
	if cfg.Worker.Capacity, err = intEnv("WORKER_CAPACITY", 4); err != nil {
		return Config{}, err
	}
	cfg.Worker.TaskTypes = split(env("WORKER_TASK_TYPES", "mock,http"))
	if cfg.Worker.GracePeriod, err = durationEnv("WORKER_GRACE_PERIOD", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Worker.FetchInterval, err = durationEnv("WORKER_FETCH_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.Worker.HeartbeatInterval, err = durationEnv("WORKER_HEARTBEAT_INTERVAL", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Worker.LeaseDuration, err = durationEnv("WORKER_LEASE_DURATION", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Worker.RenewInterval, err = durationEnv("WORKER_RENEW_INTERVAL", 10*time.Second); err != nil {
		return Config{}, err
	}
	cfg.HTTPExecutor.AllowedHosts = split(env("HTTP_EXECUTOR_ALLOWED_HOSTS", "example.com"))
	if cfg.HTTPExecutor.RequestTimeout, err = durationEnv("HTTP_EXECUTOR_REQUEST_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	requestBytes, err := intEnv("HTTP_EXECUTOR_MAX_REQUEST_BYTES", 256<<10)
	if err != nil {
		return Config{}, err
	}
	cfg.HTTPExecutor.MaxRequestBytes = int64(requestBytes)
	responseBytes, err := intEnv("HTTP_EXECUTOR_MAX_RESPONSE_BYTES", 1<<20)
	if err != nil {
		return Config{}, err
	}
	cfg.HTTPExecutor.MaxResponseBytes = int64(responseBytes)
	if cfg.HTTPExecutor.MaxRedirects, err = intEnv("HTTP_EXECUTOR_MAX_REDIRECTS", 3); err != nil {
		return Config{}, err
	}
	cfg.LLMExecutor.Provider = env("LLM_PROVIDER", "openai-compatible")
	cfg.LLMExecutor.BaseURL = strings.TrimSpace(os.Getenv("LLM_BASE_URL"))
	cfg.LLMExecutor.APIKey = os.Getenv("LLM_API_KEY")
	cfg.LLMExecutor.AllowedModels = split(os.Getenv("LLM_ALLOWED_MODELS"))
	if cfg.LLMExecutor.RequestTimeout, err = durationEnv("LLM_REQUEST_TIMEOUT", 45*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.LLMExecutor.DialTimeout, err = durationEnv("LLM_DIAL_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.LLMExecutor.TLSHandshakeTimeout, err = durationEnv("LLM_TLS_HANDSHAKE_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	promptBytes, err := intEnv("LLM_MAX_PROMPT_BYTES", 256<<10)
	if err != nil {
		return Config{}, err
	}
	cfg.LLMExecutor.MaxPromptBytes = int64(promptBytes)
	responseBytes, err = intEnv("LLM_MAX_RESPONSE_BYTES", 1<<20)
	if err != nil {
		return Config{}, err
	}
	cfg.LLMExecutor.MaxResponseBytes = int64(responseBytes)
	if cfg.LLMExecutor.MaxOutputTokens, err = intEnv("LLM_MAX_OUTPUT_TOKENS", 4096); err != nil {
		return Config{}, err
	}
	if cfg.LLMExecutor.MaxConcurrency, err = intEnv("LLM_MAX_CONCURRENCY", 4); err != nil {
		return Config{}, err
	}
	if cfg.LLMExecutor.LogContent, err = boolEnv("LLM_LOG_CONTENT", false); err != nil {
		return Config{}, err
	}
	if cfg.LLMExecutor.ToolCallingEnabled, err = boolEnv("LLM_TOOL_CALLING_ENABLED", false); err != nil {
		return Config{}, err
	}
	if cfg.LLMExecutor.MaxToolRounds, err = intEnv("LLM_MAX_TOOL_ROUNDS", 3); err != nil {
		return Config{}, err
	}
	cfg.LLMExecutor.CostTable = map[string]LLMCost{}
	if raw := strings.TrimSpace(os.Getenv("LLM_COST_TABLE_JSON")); raw != "" {
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg.LLMExecutor.CostTable); err != nil {
			return Config{}, fmt.Errorf("LLM_COST_TABLE_JSON: %w", err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return Config{}, errors.New("LLM_COST_TABLE_JSON must contain exactly one JSON object")
		}
	}
	return cfg, nil
}

func Load() (Config, error) {
	cfg, err := load()
	if err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}
func LoadWorker() (Config, error) {
	cfg, err := load()
	if err != nil {
		return Config{}, err
	}
	var errs []error
	if cfg.GRPCAddr == "" {
		errs = append(errs, errors.New("GRPC_ADDR is required"))
	}
	if cfg.Worker.Capacity <= 0 || cfg.Worker.Capacity > 1024 {
		errs = append(errs, errors.New("invalid worker capacity"))
	}
	if cfg.Worker.RenewInterval <= 0 || cfg.Worker.RenewInterval >= cfg.Worker.LeaseDuration {
		errs = append(errs, errors.New("invalid worker lease timing"))
	}
	if contains(cfg.Worker.TaskTypes, "http") && (len(cfg.HTTPExecutor.AllowedHosts) == 0 || cfg.HTTPExecutor.RequestTimeout <= 0 || cfg.HTTPExecutor.MaxRequestBytes <= 0 || cfg.HTTPExecutor.MaxResponseBytes <= 0) {
		errs = append(errs, errors.New("invalid HTTP executor configuration"))
	}
	if contains(cfg.Worker.TaskTypes, "llm") {
		errs = append(errs, cfg.validateLLM()...)
	}
	return cfg, errors.Join(errs...)
}

func (c Config) validateLLM() []error {
	var errs []error
	llm := c.LLMExecutor
	if llm.Provider != "openai-compatible" {
		errs = append(errs, errors.New("LLM_PROVIDER must be openai-compatible"))
	}
	parsed, err := url.Parse(llm.BaseURL)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		errs = append(errs, errors.New("LLM_BASE_URL must be an absolute URL without credentials, query, or fragment"))
	} else if parsed.Scheme != "https" && !(strings.EqualFold(c.AppEnv, "test") && parsed.Scheme == "http") {
		errs = append(errs, errors.New("LLM_BASE_URL must use HTTPS outside the test environment"))
	}
	if strings.TrimSpace(llm.APIKey) == "" {
		errs = append(errs, errors.New("LLM_API_KEY is required when llm tasks are enabled"))
	}
	if len(llm.AllowedModels) == 0 {
		errs = append(errs, errors.New("LLM_ALLOWED_MODELS must not be empty"))
	}
	if llm.RequestTimeout <= 0 || llm.DialTimeout <= 0 || llm.TLSHandshakeTimeout <= 0 || llm.MaxPromptBytes <= 0 || llm.MaxResponseBytes <= 0 || llm.MaxOutputTokens <= 0 || llm.MaxConcurrency <= 0 {
		errs = append(errs, errors.New("invalid LLM executor limits"))
	}
	if llm.MaxConcurrency > c.Worker.Capacity {
		errs = append(errs, errors.New("LLM_MAX_CONCURRENCY cannot exceed WORKER_CAPACITY"))
	}
	if llm.ToolCallingEnabled {
		errs = append(errs, errors.New("LLM tool calling is not available in the reliable executor baseline"))
	}
	if llm.LogContent {
		errs = append(errs, errors.New("LLM_LOG_CONTENT is not supported by the secure executor baseline"))
	}
	if llm.MaxToolRounds <= 0 || llm.MaxToolRounds > 3 {
		errs = append(errs, errors.New("LLM_MAX_TOOL_ROUNDS must be between 1 and 3"))
	}
	for model, cost := range llm.CostTable {
		if !contains(llm.AllowedModels, model) || cost.PromptMicrounitsPerMillionTokens < 0 || cost.CompletionMicrounitsPerMillionTokens < 0 {
			errs = append(errs, fmt.Errorf("invalid LLM cost entry for model %q", model))
		}
	}
	return errs
}

func (c Config) Validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if len(c.TokenPepper) < 32 {
		errs = append(errs, errors.New("TOKEN_PEPPER must contain at least 32 characters"))
	}
	if len(c.AdminToken) < 32 {
		errs = append(errs, errors.New("ADMIN_TOKEN must contain at least 32 characters"))
	}
	if c.GORM.MaxOpen <= 0 || c.GORM.MaxIdle < 0 || c.GORM.MaxIdle > c.GORM.MaxOpen {
		errs = append(errs, errors.New("invalid GORM connection pool limits"))
	}
	if c.PGX.MaxConns <= 0 || c.PGX.MinConns < 0 || c.PGX.MinConns > c.PGX.MaxConns {
		errs = append(errs, errors.New("invalid PGX connection pool limits"))
	}
	if c.Worker.Capacity <= 0 || c.Worker.Capacity > 1024 {
		errs = append(errs, errors.New("WORKER_CAPACITY must be between 1 and 1024"))
	}
	if c.HTTP.RequestTimeout <= 0 || c.HTTP.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("HTTP timeout and body limit must be positive"))
	}
	if len(c.HTTPExecutor.AllowedHosts) == 0 || c.HTTPExecutor.RequestTimeout <= 0 || c.HTTPExecutor.MaxRequestBytes <= 0 || c.HTTPExecutor.MaxResponseBytes <= 0 || c.HTTPExecutor.MaxRedirects < 0 {
		errs = append(errs, errors.New("invalid HTTP executor limits"))
	}
	if c.Worker.RenewInterval <= 0 || c.Worker.RenewInterval >= c.Worker.LeaseDuration {
		errs = append(errs, errors.New("worker renew interval must be shorter than lease duration"))
	}
	return errors.Join(errs...)
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
func split(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
func intEnv(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func boolEnv(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return value, nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
