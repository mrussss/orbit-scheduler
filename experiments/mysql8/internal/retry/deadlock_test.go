package retry

import (
	"context"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

func TestDeadlockClassificationAndDelay(t *testing.T) {
	deadlock := &drivermysql.MySQLError{Number: 1213, Message: "deadlock"}
	if !IsDeadlock(deadlock) {
		t.Fatal("deadlock not classified")
	}
	if IsDeadlock(&drivermysql.MySQLError{Number: 1205, Message: "lock wait timeout"}) {
		t.Fatal("lock wait timeout classified as deadlock")
	}
	for retry := 0; retry < 5; retry++ {
		delay := exponentialDelay(time.Millisecond, retry)
		floor := time.Millisecond * time.Duration(1<<retry)
		if delay < floor || delay >= 2*floor {
			t.Fatalf("retry=%d delay=%s", retry, delay)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WithDeadlockRetry(ctx, nil, 1, time.Millisecond, nil); err == nil {
		t.Fatal("invalid retry configuration accepted")
	}
}
