package observability

import (
	"testing"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSchedulerMetricsRecordBoundedOutcomesAndState(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewSchedulerMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.Fetch("success", 3, time.Millisecond)
	metrics.Renew("stale_lease", time.Millisecond)
	metrics.Report("conflict", time.Millisecond)
	metrics.Reaper(scheduler.ReapResult{Requeued: 2, Failed: 1, Canceled: 1}, time.Millisecond)
	metrics.SetTaskStatus("RUNNING", 4)
	if got := testutil.ToFloat64(metrics.fetches.WithLabelValues("success")); got != 1 {
		t.Fatalf("fetches=%f", got)
	}
	if got := testutil.ToFloat64(metrics.renews.WithLabelValues("stale_lease")); got != 1 {
		t.Fatalf("renews=%f", got)
	}
	if got := testutil.ToFloat64(metrics.reports.WithLabelValues("conflict")); got != 1 {
		t.Fatalf("reports=%f", got)
	}
	if got := testutil.ToFloat64(metrics.reaperProcessed.WithLabelValues("requeued")); got != 2 {
		t.Fatalf("requeued=%f", got)
	}
	if got := testutil.ToFloat64(metrics.taskStatus.WithLabelValues("RUNNING")); got != 4 {
		t.Fatalf("running tasks=%f", got)
	}
}
