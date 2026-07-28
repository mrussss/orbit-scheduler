package auth

import (
	"strings"
	"testing"
)

func TestTokenGenerationAndHash(t *testing.T) {
	codec, err := NewTokenCodec(strings.Repeat("p", 32))
	if err != nil {
		t.Fatal(err)
	}
	plain, prefix, hash, err := codec.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) < 40 || prefix != plain[:prefixLength] {
		t.Fatalf("plain=%q prefix=%q", plain, prefix)
	}
	again := codec.Hash(plain)
	if !Equal(hash[:], again[:]) {
		t.Fatal("hash is not stable")
	}
	other, _ := NewTokenCodec(strings.Repeat("q", 32))
	different := other.Hash(plain)
	if Equal(hash[:], different[:]) {
		t.Fatal("pepper did not affect hash")
	}
}
