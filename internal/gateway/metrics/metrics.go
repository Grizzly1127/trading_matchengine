package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics Gateway Prometheus 指标。
type Metrics struct {
	placeOrder prometheus.Histogram
}

var placeOrderBuckets = []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500}

// New 注册 Prometheus 指标。
func New() *Metrics {
	m := &Metrics{
		placeOrder: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_place_order_duration_ms",
			Help:    "POST /v1/orders handler wall time in milliseconds",
			Buckets: placeOrderBuckets,
		}),
	}
	prometheus.MustRegister(m.placeOrder)
	return m
}

// ObservePlaceOrder 记录下单 HTTP 处理耗时。
func (m *Metrics) ObservePlaceOrder(d time.Duration) {
	if m != nil && d > 0 {
		m.placeOrder.Observe(float64(d.Milliseconds()))
	}
}
