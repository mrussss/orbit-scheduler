package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"
)

const prefixLength = 16

type TokenCodec struct{ pepper []byte }

func NewTokenCodec(pepper string) (*TokenCodec, error) {
	if len(pepper) < 32 {
		return nil, errors.New("token pepper must contain at least 32 characters")
	}
	return &TokenCodec{pepper: []byte(pepper)}, nil
}

func (c *TokenCodec) Generate() (plain, prefix string, hash [32]byte, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", hash, err
	}
	plain = "orb_" + base64.RawURLEncoding.EncodeToString(raw)
	prefix = plain[:prefixLength]
	hash = c.Hash(plain)
	return
}

func (c *TokenCodec) Hash(plain string) [32]byte {
	mac := hmac.New(sha256.New, c.pepper)
	_, _ = mac.Write([]byte(plain))
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}
func Prefix(plain string) (string, error) {
	plain = strings.TrimSpace(plain)
	if len(plain) < prefixLength || !strings.HasPrefix(plain, "orb_") {
		return "", errors.New("invalid token")
	}
	return plain[:prefixLength], nil
}
func Equal(a, b []byte) bool { return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1 }
