package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics Order Service Prometheus 指标。
type Metrics struct {
	outboxPending        prometheus.Gauge
	stuckPending         prometheus.Gauge
	relayBatchSize       prometheus.Histogram
	relayDispatchLatency prometheus.Histogram
	relayPublished       prometheus.Counter
}

// New 注册 Prometheus 指标。
func New() *Metrics {
	m := &Metrics{
		outboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "order_outbox_pending_count",
			Help: "Unpublished rows in order_outbox",
		}),
		stuckPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "order_stuck_pending_seconds",
			Help: "Max seconds a published-but-still-PENDING order has been waiting",
		}),
		relayBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "order_outbox_relay_batch_size",
			Help:    "Outbox relay claimed batch size per poll",
			Buckets: prometheus.LinearBuckets(0, 50, 11),
		}),
		relayDispatchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "order_outbox_relay_dispatch_latency_ms",
			Help:    "Outbox relay batch dispatch latency in milliseconds (claim through commit)",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		relayPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "order_outbox_relay_published_total",
			Help: "Total outbox rows marked published by relay",
		}),
	}
	prometheus.MustRegister(
		m.outboxPending,
		m.stuckPending,
		m.relayBatchSize,
		m.relayDispatchLatency,
		m.relayPublished,
	)
	return m
}

func (m *Metrics) SetOutboxPendingCount(n int) {
	if m != nil {
		m.outboxPending.Set(float64(n))
	}
}

func (m *Metrics) SetOrderStuckPendingSeconds(sec float64) {
	if m != nil {
		m.stuckPending.Set(sec)
	}
}

// ObserveRelayBatchSize 记录单批领取条数。
func (m *Metrics) ObserveRelayBatchSize(n int) {
	if m != nil && n > 0 {
		m.relayBatchSize.Observe(float64(n))
	}
}

// ObserveRelayDispatchLatencyMs 记录单批 fetch→kafka→mark 耗时（毫秒）。
func (m *Metrics) ObserveRelayDispatchLatencyMs(ms float64) {
	if m != nil && ms >= 0 {
		m.relayDispatchLatency.Observe(ms)
	}
}

// AddRelayPublished 累计 Relay 标记已发布条数。
func (m *Metrics) AddRelayPublished(n int) {
	if m != nil && n > 0 {
		m.relayPublished.Add(float64(n))
	}
}
