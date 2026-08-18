package llm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	defaultMaxMessages     = 128
	defaultMaxMessageBytes = 64 << 10
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Payload struct {
	Model           string            `json:"model"`
	Messages        []Message         `json:"messages"`
	Temperature     *float64          `json:"temperature,omitempty"`
	MaxOutputTokens int               `json:"max_output_tokens,omitempty"`
	ResponseFormat  string            `json:"response_format,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

type PayloadPolicy struct {
	AllowedModels   map[string]struct{}
	MaxPromptBytes  int64
	MaxOutputTokens int
	MaxMessages     int
	MaxMessageBytes int
}

func ParsePayload(raw []byte, policy PayloadPolicy) (Payload, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload Payload
	if err := decoder.Decode(&payload); err != nil {
		return Payload{}, fmt.Errorf("decode llm payload: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Payload{}, err
	}
	if err := payload.Validate(policy); err != nil {
		return Payload{}, err
	}
	return payload, nil
}

func (p Payload) Validate(policy PayloadPolicy) error {
	if _, ok := policy.AllowedModels[p.Model]; !ok || p.Model == "" {
		return errors.New("model is not allowed")
	}
	maxMessages := policy.MaxMessages
	if maxMessages <= 0 {
		maxMessages = defaultMaxMessages
	}
	if len(p.Messages) == 0 || len(p.Messages) > maxMessages {
		return fmt.Errorf("messages count must be between 1 and %d", maxMessages)
	}
	maxMessageBytes := policy.MaxMessageBytes
	if maxMessageBytes <= 0 {
		maxMessageBytes = defaultMaxMessageBytes
	}
	var promptBytes int64
	for index, message := range p.Messages {
		if message.Role != "system" && message.Role != "user" && message.Role != "assistant" {
			return fmt.Errorf("messages[%d].role is invalid", index)
		}
		if message.Content == "" || !utf8.ValidString(message.Content) {
			return fmt.Errorf("messages[%d].content is invalid", index)
		}
		if len(message.Content) > maxMessageBytes {
			return fmt.Errorf("messages[%d].content is too large", index)
		}
		promptBytes += int64(len(message.Content))
	}
	if policy.MaxPromptBytes <= 0 || promptBytes > policy.MaxPromptBytes {
		return errors.New("prompt is too large")
	}
	if p.Temperature != nil && (*p.Temperature < 0 || *p.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	if p.MaxOutputTokens <= 0 || p.MaxOutputTokens > policy.MaxOutputTokens {
		return errors.New("max_output_tokens is outside the configured limit")
	}
	if p.ResponseFormat != "" && p.ResponseFormat != "json_object" {
		return errors.New("response_format must be json_object when present")
	}
	if len(p.Metadata) > 32 {
		return errors.New("metadata contains too many entries")
	}
	for key, value := range p.Metadata {
		if key == "" || len(key) > 128 || len(value) > 1024 {
			return errors.New("metadata key or value is too large")
		}
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("llm payload contains trailing JSON")
		}
		return fmt.Errorf("decode llm payload trailing content: %w", err)
	}
	return nil
}
