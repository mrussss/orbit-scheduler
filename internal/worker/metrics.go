package worker

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

type Observer interface {
	Fetch(outcome string, count int, durationSeconds float64)
	TaskStarted(taskType string)
	TaskFinished(taskType string)
	ExecutorFinished(taskType, outcome string, durationSeconds float64)
	RenewError()
	ReportRetry()
	Shutdown(durationSeconds float64)
}

type PrometheusObserver struct {
	fetches          *prometheus.CounterVec
	fetchBatch       prometheus.Histogram
	fetchDuration    *prometheus.HistogramVec
	running          *prometheus.GaugeVec
	executorDuration *prometheus.HistogramVec
	renewErrors      prometheus.Counter
	reportRetries    prometheus.Counter
	shutdownDuration prometheus.Histogram
}

func NewPrometheusObserver(registerer prometheus.Registerer) (*PrometheusObserver, error) {
	if registerer == nil {
		return nil, errors.New("worker metrics registerer is required")
	}
	observer := &PrometheusObserver{
		fetches:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_worker_fetch_total", Help: "Worker fetch calls by bounded outcome."}, []string{"outcome"}),
		fetchBatch:       prometheus.NewHistogram(prometheus.HistogramOpts{Name: "orbit_worker_fetch_batch_size", Help: "Number of assignments returned by successful fetch calls.", Buckets: prometheus.ExponentialBuckets(1, 2, 8)}),
		fetchDuration:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "orbit_worker_fetch_duration_seconds", Help: "Worker fetch call duration by bounded outcome.", Buckets: prometheus.DefBuckets}, []string{"outcome"}),
		running:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "orbit_worker_running_tasks", Help: "Tasks currently owned by this worker process by configured task type."}, []string{"task_type"}),
		executorDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "orbit_worker_executor_duration_seconds", Help: "Executor duration by configured task type and Orbit outcome.", Buckets: prometheus.DefBuckets}, []string{"task_type", "outcome"}),
		renewErrors:      prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_worker_renew_errors_total", Help: "Lease renewal errors observed by this worker."}),
		reportRetries:    prometheus.NewCounter(prometheus.CounterOpts{Name: "orbit_worker_report_retries_total", Help: "Result report retries attempted by this worker."}),
		shutdownDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "orbit_worker_shutdown_duration_seconds", Help: "Worker shutdown duration.", Buckets: prometheus.DefBuckets}),
	}
	collectors := []prometheus.Collector{observer.fetches, observer.fetchBatch, observer.fetchDuration, observer.running, observer.executorDuration, observer.renewErrors, observer.reportRetries, observer.shutdownDuration}
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return observer, nil
}

func (m *PrometheusObserver) Fetch(outcome string, count int, durationSeconds float64) {
	m.fetches.WithLabelValues(outcome).Inc()
	m.fetchDuration.WithLabelValues(outcome).Observe(durationSeconds)
	if outcome == "success" || outcome == "empty" {
		m.fetchBatch.Observe(float64(count))
	}
}
func (m *PrometheusObserver) TaskStarted(taskType string)  { m.running.WithLabelValues(taskType).Inc() }
func (m *PrometheusObserver) TaskFinished(taskType string) { m.running.WithLabelValues(taskType).Dec() }
func (m *PrometheusObserver) ExecutorFinished(taskType, outcome string, durationSeconds float64) {
	m.executorDuration.WithLabelValues(taskType, outcome).Observe(durationSeconds)
}
func (m *PrometheusObserver) RenewError()  { m.renewErrors.Inc() }
func (m *PrometheusObserver) ReportRetry() { m.reportRetries.Inc() }
func (m *PrometheusObserver) Shutdown(durationSeconds float64) {
	m.shutdownDuration.Observe(durationSeconds)
}

type noopObserver struct{}

func (noopObserver) Fetch(string, int, float64)               {}
func (noopObserver) TaskStarted(string)                       {}
func (noopObserver) TaskFinished(string)                      {}
func (noopObserver) ExecutorFinished(string, string, float64) {}
func (noopObserver) RenewError()                              {}
func (noopObserver) ReportRetry()                             {}
func (noopObserver) Shutdown(float64)                         {}
