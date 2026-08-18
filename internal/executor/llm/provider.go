package llm

import (
	"context"
	"errors"
	"fmt"
)

type Request struct {
	Model           string
	Messages        []Message
	Temperature     *float64
	MaxOutputTokens int
	ResponseFormat  string
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type Response struct {
	Model        string
	Content      string
	FinishReason string
	Usage        Usage
}

type Provider interface {
	Generate(context.Context, Request) (Response, error)
}

type ErrorKind string

const (
	ErrorRateLimited      ErrorKind = "RATE_LIMITED"
	ErrorUpstream         ErrorKind = "UPSTREAM"
	ErrorTransport        ErrorKind = "TRANSPORT"
	ErrorInvalidRequest   ErrorKind = "INVALID_REQUEST"
	ErrorAuthentication   ErrorKind = "AUTHENTICATION"
	ErrorModelNotFound    ErrorKind = "MODEL_NOT_FOUND"
	ErrorInvalidResponse  ErrorKind = "INVALID_RESPONSE"
	ErrorResponseTooLarge ErrorKind = "RESPONSE_TOO_LARGE"
	ErrorTimeout          ErrorKind = "TIMEOUT"
	ErrorCanceled         ErrorKind = "CANCELED"
)

type ProviderError struct {
	Kind       ErrorKind
	StatusCode int
	Retryable  bool
	Cause      error
}

func (e *ProviderError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("llm provider %s (status %d)", e.Kind, e.StatusCode)
	}
	return fmt.Sprintf("llm provider %s", e.Kind)
}

func (e *ProviderError) Unwrap() error { return e.Cause }

func AsProviderError(err error) (*ProviderError, bool) {
	var providerError *ProviderError
	ok := errors.As(err, &providerError)
	return providerError, ok
}
