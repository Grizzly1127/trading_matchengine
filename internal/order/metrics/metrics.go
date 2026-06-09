package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics Order Service Prometheus 指标。
type Metrics struct {
	outboxPending        prometheus.Gauge
	stuckPending         prometheus.Gauge
	relayBatchSize       prometheus.Histogram
	relayDispatchLatency prometheus.Histogram
	relayPublished       prometheus.Counter

	grpcPlaceOrder   prometheus.Histogram
	placeValidate    prometheus.Histogram
	placeIdempotency prometheus.Histogram
	placeDBTx        prometheus.Histogram
}

var placeOrderBuckets = []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500}

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
		grpcPlaceOrder: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "order_grpc_place_order_duration_ms",
			Help:    "PlaceOrder gRPC handler wall time in milliseconds",
			Buckets: placeOrderBuckets,
		}),
		placeValidate: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "order_place_order_validate_ms",
			Help:    "PlaceOrder validatePlaceOrder wall time in milliseconds",
			Buckets: placeOrderBuckets,
		}),
		placeIdempotency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "order_place_order_idempotency_ms",
			Help:    "PlaceOrder idempotency lookup wall time in milliseconds",
			Buckets: placeOrderBuckets,
		}),
		placeDBTx: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "order_place_order_db_tx_ms",
			Help:    "PlaceOrder InsertPending transaction wall time in milliseconds",
			Buckets: placeOrderBuckets,
		}),
	}
	prometheus.MustRegister(
		m.outboxPending,
		m.stuckPending,
		m.relayBatchSize,
		m.relayDispatchLatency,
		m.relayPublished,
		m.grpcPlaceOrder,
		m.placeValidate,
		m.placeIdempotency,
		m.placeDBTx,
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

// ObserveGRPCPlaceOrder 记录 PlaceOrder gRPC 总耗时。
func (m *Metrics) ObserveGRPCPlaceOrder(d time.Duration) {
	if m != nil && d > 0 {
		m.grpcPlaceOrder.Observe(float64(d.Milliseconds()))
	}
}

// ObservePlaceValidate 记录参数校验耗时。
func (m *Metrics) ObservePlaceValidate(d time.Duration) {
	if m != nil && d > 0 {
		m.placeValidate.Observe(float64(d.Milliseconds()))
	}
}

// ObservePlaceIdempotency 记录幂等查询耗时。
func (m *Metrics) ObservePlaceIdempotency(d time.Duration) {
	if m != nil && d > 0 {
		m.placeIdempotency.Observe(float64(d.Milliseconds()))
	}
}

// ObservePlaceDBTx 记录 InsertPending 事务耗时。
func (m *Metrics) ObservePlaceDBTx(d time.Duration) {
	if m != nil && d > 0 {
		m.placeDBTx.Observe(float64(d.Milliseconds()))
	}
}
