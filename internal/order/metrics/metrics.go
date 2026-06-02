package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics Order Service Prometheus 指标。
type Metrics struct {
	outboxPending prometheus.Gauge
	stuckPending  prometheus.Gauge
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
	}
	prometheus.MustRegister(m.outboxPending, m.stuckPending)
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
