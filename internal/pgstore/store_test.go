package pgstore

import (
	"testing"
	"time"
)

func TestNewValidatesConfig(t *testing.T) {
	_, err := New(nil, Config{MaxFetchBatch: 10, RetryBase: time.Second, RetryMax: time.Minute})
	if err == nil {
		t.Fatal("expected missing pool error")
	}
}
