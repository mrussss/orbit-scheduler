package llm

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

type Observer interface {
	Started(provider, model string)
	Finished(provider, model, outcome string, durationSeconds float64, usage Usage, estimatedCostMicrounits *int64, rateLimited bool)
}

type PrometheusObserver struct {
	requests    *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	tokens      *prometheus.CounterVec
	cost        *prometheus.CounterVec
	rateLimited *prometheus.CounterVec
	inFlight    *prometheus.GaugeVec
}

func NewPrometheusObserver(registerer prometheus.Registerer) (*PrometheusObserver, error) {
	if registerer == nil {
		return nil, errors.New("LLM metrics registerer is required")
	}
	observer := &PrometheusObserver{
		requests:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_llm_requests_total", Help: "LLM requests by configured provider, model, and Orbit outcome."}, []string{"provider", "model", "outcome"}),
		duration:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "orbit_llm_request_duration_seconds", Help: "LLM provider request duration by configured provider and model.", Buckets: prometheus.DefBuckets}, []string{"provider", "model"}),
		tokens:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_llm_tokens_total", Help: "LLM usage tokens by configured provider, model, and token type."}, []string{"provider", "model", "type"}),
		cost:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_llm_estimated_cost_microunits_total", Help: "Estimated LLM cost in configured integer microunits."}, []string{"provider", "model"}),
		rateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "orbit_llm_rate_limited_total", Help: "LLM requests rejected by upstream rate limiting."}, []string{"provider", "model"}),
		inFlight:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "orbit_llm_in_flight", Help: "Current LLM requests by configured provider and model."}, []string{"provider", "model"}),
	}
	for _, collector := range []prometheus.Collector{observer.requests, observer.duration, observer.tokens, observer.cost, observer.rateLimited, observer.inFlight} {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return observer, nil
}

func (m *PrometheusObserver) Started(provider, model string) {
	m.inFlight.WithLabelValues(provider, model).Inc()
}

func (m *PrometheusObserver) Finished(provider, model, outcome string, durationSeconds float64, usage Usage, estimatedCostMicrounits *int64, rateLimited bool) {
	m.inFlight.WithLabelValues(provider, model).Dec()
	m.requests.WithLabelValues(provider, model, outcome).Inc()
	m.duration.WithLabelValues(provider, model).Observe(durationSeconds)
	if usage.PromptTokens > 0 {
		m.tokens.WithLabelValues(provider, model, "prompt").Add(float64(usage.PromptTokens))
	}
	if usage.CompletionTokens > 0 {
		m.tokens.WithLabelValues(provider, model, "completion").Add(float64(usage.CompletionTokens))
	}
	if estimatedCostMicrounits != nil && *estimatedCostMicrounits > 0 {
		m.cost.WithLabelValues(provider, model).Add(float64(*estimatedCostMicrounits))
	}
	if rateLimited {
		m.rateLimited.WithLabelValues(provider, model).Inc()
	}
}

type noopObserver struct{}

func (noopObserver) Started(string, string)                                        {}
func (noopObserver) Finished(string, string, string, float64, Usage, *int64, bool) {}
