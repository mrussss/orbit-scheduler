package observability

import (
	"errors"
	"time"

	"github.com/mrussss/orbit-scheduler/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
)

type SchedulerMetrics struct {
	fetches         *prometheus.CounterVec
	fetchBatch      prometheus.Histogram
	fetchDuration   *prometheus.HistogramVec
	renews          *prometheus.CounterVec
	reports         *prometheus.CounterVec
	reaperProcessed *prometheus.CounterVec
	reaperDuration  prometheus.Histogram
	taskStatus      *prometheus.GaugeVec
}

func NewSchedulerMetrics(registerer prometheus.Registerer) (*SchedulerMetrics, error) {
	if registerer == nil {
		return nil, errors.New("scheduler metrics registerer is required")
	}
	metrics := &SchedulerMetrics{
		fetches:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_scheduler_fetch_total", Help: "Scheduler fetch RPCs by bounded outcome."}, []string{"outcome"}),
		fetchBatch:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "orbit_scheduler_fetch_batch_size", Help: "Tasks returned by successful scheduler fetch calls.", Buckets: prometheus.ExponentialBuckets(1, 2, 8)}),
		fetchDuration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "orbit_scheduler_fetch_duration_seconds", Help: "Scheduler fetch duration by bounded outcome.", Buckets: prometheus.DefBuckets}, []string{"outcome"}),
		renews:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_scheduler_renew_total", Help: "Scheduler renew RPCs by bounded outcome."}, []string{"outcome"}),
		reports:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_scheduler_report_total", Help: "Scheduler report RPCs by bounded outcome."}, []string{"outcome"}),
		reaperProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_scheduler_reaper_processed_total", Help: "Expired leases processed by reaper decision."}, []string{"decision"}),
		reaperDuration:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "orbit_scheduler_reaper_duration_seconds", Help: "Lease reaper pass duration.", Buckets: prometheus.DefBuckets}),
		taskStatus:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "orbit_scheduler_tasks", Help: "Current authoritative task count by bounded status."}, []string{"status"}),
	}
	collectors := []prometheus.Collector{metrics.fetches, metrics.fetchBatch, metrics.fetchDuration, metrics.renews, metrics.reports, metrics.reaperProcessed, metrics.reaperDuration, metrics.taskStatus}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

func (m *SchedulerMetrics) Fetch(outcome string, count int, duration time.Duration) {
	m.fetches.WithLabelValues(outcome).Inc()
	m.fetchDuration.WithLabelValues(outcome).Observe(duration.Seconds())
	if outcome == "success" {
		m.fetchBatch.Observe(float64(count))
	}
}

func (m *SchedulerMetrics) Renew(outcome string, duration time.Duration) {
	m.renews.WithLabelValues(outcome).Inc()
}

func (m *SchedulerMetrics) Report(outcome string, duration time.Duration) {
	m.reports.WithLabelValues(outcome).Inc()
}

func (m *SchedulerMetrics) Reaper(result scheduler.ReapResult, duration time.Duration) {
	m.reaperDuration.Observe(duration.Seconds())
	if result.Requeued > 0 {
		m.reaperProcessed.WithLabelValues("requeued").Add(float64(result.Requeued))
	}
	if result.Failed > 0 {
		m.reaperProcessed.WithLabelValues("failed").Add(float64(result.Failed))
	}
	if result.Canceled > 0 {
		m.reaperProcessed.WithLabelValues("canceled").Add(float64(result.Canceled))
	}
}

func (m *SchedulerMetrics) SetTaskStatus(status string, count float64) {
	m.taskStatus.WithLabelValues(status).Set(count)
}
