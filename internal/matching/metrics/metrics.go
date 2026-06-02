package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics 撮合进程指标（Prometheus + 进程内快照）。
type Metrics struct {
	CommandsProcessed   atomic.Uint64
	CommandsFailed      atomic.Uint64
	PublishErrors       atomic.Uint64
	TradesGenerated     atomic.Uint64
	LastProcessedOffset atomic.Uint64
	KafkaLag            atomic.Int64
	WalLastSeq          atomic.Uint64

	processingLatency prometheus.Histogram
	walAppendLatency  prometheus.Histogram
}

// New 创建指标并在默认 Registerer 上注册。
func New() *Metrics {
	m := &Metrics{
		processingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_processing_latency_ms",
			Help:    "End-to-end command processing latency in milliseconds (decode, WAL, match, publish)",
			Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
		}),
		walAppendLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_wal_append_latency_ms",
			Help:    "WAL append + fsync latency in milliseconds per command",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500},
		}),
	}
	registerCounter("matching_commands_processed_total", "Successfully processed order.commands messages", &m.CommandsProcessed)
	registerCounter("matching_commands_failed_total", "Failed order.commands processing attempts", &m.CommandsFailed)
	registerCounter("matching_publish_errors_total", "Kafka event publish failures", &m.PublishErrors)
	registerCounter("matching_trades_generated_total", "Trades produced by matching", &m.TradesGenerated)
	registerGaugeUint64("matching_kafka_last_processed_offset", "Last committed Kafka offset on command partition", &m.LastProcessedOffset)
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "matching_kafka_lag",
		Help: "Kafka consumer lag (high watermark - last processed)",
	}, func() float64 {
		return float64(m.KafkaLag.Load())
	}))
	registerGaugeUint64("matching_wal_last_seq", "Last WAL sequence number applied", &m.WalLastSeq)
	prometheus.MustRegister(m.processingLatency, m.walAppendLatency)
	return m
}

// ObserveProcessing 记录单条命令端到端耗时。
func (m *Metrics) ObserveProcessing(d time.Duration) {
	if m == nil {
		return
	}
	m.CommandsProcessed.Add(1)
	m.processingLatency.Observe(float64(d.Milliseconds()))
}

// ObserveWalAppend 记录 WAL 落盘耗时。
func (m *Metrics) ObserveWalAppend(d time.Duration) {
	if m == nil {
		return
	}
	m.walAppendLatency.Observe(float64(d.Milliseconds()))
}

// ObserveCommandFailed 处理失败计数。
func (m *Metrics) ObserveCommandFailed() {
	if m != nil {
		m.CommandsFailed.Add(1)
	}
}

// ObservePublishError 发布事件失败。
func (m *Metrics) ObservePublishError() {
	if m != nil {
		m.PublishErrors.Add(1)
	}
}

// ObserveTrades 记录本命令产生的成交笔数。
func (m *Metrics) ObserveTrades(n int) {
	if m != nil && n > 0 {
		m.TradesGenerated.Add(uint64(n))
	}
}

// SetLastProcessedOffset 更新已处理 Kafka offset。
func (m *Metrics) SetLastProcessedOffset(offset uint64) {
	if m != nil {
		m.LastProcessedOffset.Store(offset)
	}
}

// SetKafkaLag 更新 lag（由后台轮询 ReadLag 写入）。
func (m *Metrics) SetKafkaLag(lag int64) {
	if m != nil {
		m.KafkaLag.Store(lag)
	}
}

// SetWalLastSeq 更新 WAL 序号。
func (m *Metrics) SetWalLastSeq(seq uint64) {
	if m != nil {
		m.WalLastSeq.Store(seq)
	}
}

// Snapshot 进程内计数快照（结构化日志用）。
type Snapshot struct {
	CommandsProcessed   uint64
	CommandsFailed      uint64
	PublishErrors       uint64
	TradesGenerated     uint64
	LastProcessedOffset uint64
	KafkaLag            int64
	WalLastSeq          uint64
}

func (m *Metrics) Snap() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	return Snapshot{
		CommandsProcessed:   m.CommandsProcessed.Load(),
		CommandsFailed:      m.CommandsFailed.Load(),
		PublishErrors:       m.PublishErrors.Load(),
		TradesGenerated:     m.TradesGenerated.Load(),
		LastProcessedOffset: m.LastProcessedOffset.Load(),
		KafkaLag:            m.KafkaLag.Load(),
		WalLastSeq:          m.WalLastSeq.Load(),
	}
}

func registerCounter(name, help string, v *atomic.Uint64) {
	c := prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 {
		return float64(v.Load())
	})
	prometheus.MustRegister(c)
}

func registerGaugeUint64(name, help string, v *atomic.Uint64) {
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
		return float64(v.Load())
	})
	prometheus.MustRegister(g)
}
