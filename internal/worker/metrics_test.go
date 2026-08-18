package worker

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPrometheusObserverRecordsBoundedWorkerMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer, err := NewPrometheusObserver(registry)
	if err != nil {
		t.Fatal(err)
	}
	observer.Fetch("empty", 0, time.Millisecond.Seconds())
	observer.TaskStarted("llm")
	observer.ExecutorFinished("llm", "SUCCEEDED", time.Second.Seconds())
	observer.RenewError()
	observer.ReportRetry()
	observer.TaskFinished("llm")
	observer.Shutdown(time.Second.Seconds())
	if got := testutil.ToFloat64(observer.fetches.WithLabelValues("empty")); got != 1 {
		t.Fatalf("fetches=%f", got)
	}
	if got := testutil.ToFloat64(observer.running.WithLabelValues("llm")); got != 0 {
		t.Fatalf("running=%f", got)
	}
	if got := testutil.ToFloat64(observer.renewErrors); got != 1 {
		t.Fatalf("renew errors=%f", got)
	}
	if got := testutil.ToFloat64(observer.reportRetries); got != 1 {
		t.Fatalf("report retries=%f", got)
	}
}
