package llm

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type OpenAICompatibleConfig struct {
	BaseURL             string
	APIKey              string
	RequestTimeout      time.Duration
	DialTimeout         time.Duration
	TLSHandshakeTimeout time.Duration
	MaxResponseBytes    int64
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	AllowHTTP           bool
}

type OpenAICompatible struct {
	endpoint         string
	apiKey           string
	maxResponseBytes int64
	client           *http.Client
}

func NewOpenAICompatible(cfg OpenAICompatibleConfig) (*OpenAICompatible, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || !base.IsAbs() || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("invalid OpenAI-compatible base URL")
	}
	if base.Scheme != "https" && !(cfg.AllowHTTP && base.Scheme == "http") {
		return nil, errors.New("OpenAI-compatible base URL must use HTTPS")
	}
	if strings.TrimSpace(cfg.APIKey) == "" || cfg.RequestTimeout <= 0 || cfg.DialTimeout <= 0 || cfg.TLSHandshakeTimeout <= 0 || cfg.MaxResponseBytes <= 0 {
		return nil, errors.New("invalid OpenAI-compatible provider configuration")
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 100
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = 16
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		ExpectContinueTimeout: time.Second,
	}
	return &OpenAICompatible{
		endpoint:         completionEndpoint(base),
		apiKey:           cfg.APIKey,
		maxResponseBytes: cfg.MaxResponseBytes,
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.RequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

type openAIRequest struct {
	Model               string         `json:"model"`
	Messages            []Message      `json:"messages"`
	Temperature         *float64       `json:"temperature,omitempty"`
	MaxCompletionTokens int            `json:"max_completion_tokens"`
	ResponseFormat      map[string]any `json:"response_format,omitempty"`
	Stream              bool           `json:"stream"`
}

type openAIResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage `json:"usage"`
}

func (p *OpenAICompatible) Generate(ctx context.Context, input Request) (Response, error) {
	payload := openAIRequest{Model: input.Model, Messages: input.Messages, Temperature: input.Temperature, MaxCompletionTokens: input.MaxOutputTokens, Stream: false}
	if input.ResponseFormat != "" {
		payload.ResponseFormat = map[string]any{"type": input.ResponseFormat}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Response{}, &ProviderError{Kind: ErrorInvalidRequest, Cause: err}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return Response{}, &ProviderError{Kind: ErrorInvalidRequest, Cause: err}
	}
	request.Header.Set("Authorization", "Bearer "+p.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(request)
	if err != nil {
		return Response{}, classifyTransportError(ctx, err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, p.maxResponseBytes+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return Response{}, &ProviderError{Kind: ErrorTransport, Retryable: true, Cause: err}
	}
	if int64(len(responseBody)) > p.maxResponseBytes {
		return Response{}, &ProviderError{Kind: ErrorResponseTooLarge, StatusCode: response.StatusCode}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode > 299 {
		return Response{}, classifyStatus(response.StatusCode)
	}
	var decoded openAIResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return Response{}, &ProviderError{Kind: ErrorInvalidResponse, StatusCode: response.StatusCode, Retryable: true, Cause: err}
	}
	if decoded.Model == "" || len(decoded.Choices) == 0 || decoded.Usage == nil {
		return Response{}, &ProviderError{Kind: ErrorInvalidResponse, StatusCode: response.StatusCode, Retryable: true}
	}
	if decoded.Usage.PromptTokens < 0 || decoded.Usage.CompletionTokens < 0 || decoded.Usage.TotalTokens < 0 {
		return Response{}, &ProviderError{Kind: ErrorInvalidResponse, StatusCode: response.StatusCode, Retryable: true}
	}
	choice := decoded.Choices[0]
	if choice.FinishReason == "" {
		return Response{}, &ProviderError{Kind: ErrorInvalidResponse, StatusCode: response.StatusCode, Retryable: true}
	}
	return Response{Model: decoded.Model, Content: choice.Message.Content, FinishReason: choice.FinishReason, Usage: *decoded.Usage}, nil
}

func (p *OpenAICompatible) CloseIdleConnections() { p.client.CloseIdleConnections() }

func completionEndpoint(base *url.URL) string {
	cleaned := strings.TrimSuffix(base.Path, "/")
	switch {
	case strings.HasSuffix(cleaned, "/chat/completions"):
		base.Path = cleaned
	case strings.HasSuffix(cleaned, "/v1"):
		base.Path = path.Join(cleaned, "chat/completions")
	default:
		base.Path = path.Join(cleaned, "v1/chat/completions")
	}
	return base.String()
}

func classifyStatus(status int) *ProviderError {
	switch {
	case status == http.StatusTooManyRequests:
		return &ProviderError{Kind: ErrorRateLimited, StatusCode: status, Retryable: true}
	case status >= 500:
		return &ProviderError{Kind: ErrorUpstream, StatusCode: status, Retryable: true}
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return &ProviderError{Kind: ErrorAuthentication, StatusCode: status}
	case status == http.StatusNotFound:
		return &ProviderError{Kind: ErrorModelNotFound, StatusCode: status}
	default:
		return &ProviderError{Kind: ErrorInvalidRequest, StatusCode: status}
	}
}

func classifyTransportError(ctx context.Context, err error) *ProviderError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &ProviderError{Kind: ErrorTimeout, Retryable: true, Cause: context.DeadlineExceeded}
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return &ProviderError{Kind: ErrorCanceled, Cause: context.Canceled}
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return &ProviderError{Kind: ErrorTimeout, Retryable: true, Cause: err}
	}
	return &ProviderError{Kind: ErrorTransport, Retryable: true, Cause: fmt.Errorf("provider request failed: %w", err)}
}
