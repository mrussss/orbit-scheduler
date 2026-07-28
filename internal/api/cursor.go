package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}
type CursorCodec struct{ key []byte }

func NewCursorCodec(key string) *CursorCodec { return &CursorCodec{key: []byte(key)} }
func (c *CursorCodec) Encode(value Cursor) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	signed := append(body, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(signed), nil
}
func (c *CursorCodec) Decode(raw string) (Cursor, error) {
	var out Cursor
	signed, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(signed) <= sha256.Size {
		return out, errors.New("invalid cursor")
	}
	body, sig := signed[:len(signed)-sha256.Size], signed[len(signed)-sha256.Size:]
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return out, errors.New("invalid cursor signature")
	}
	if err := json.Unmarshal(body, &out); err != nil || out.ID == uuid.Nil || out.CreatedAt.IsZero() {
		return Cursor{}, errors.New("invalid cursor payload")
	}
	return out, nil
}
