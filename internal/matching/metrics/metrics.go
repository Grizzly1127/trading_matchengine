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
	EventOutboxPending  atomic.Uint64
	EventRelayPublished atomic.Uint64

	processingLatency      prometheus.Histogram
	walAppendLatency       prometheus.Histogram
	walSyncLatency         prometheus.Histogram
	walSyncBatchRecords    prometheus.Histogram
	eventOutboxSyncLatency prometheus.Histogram
	publishLatency         prometheus.Histogram
	publishMatchLatency    prometheus.Histogram
	publishTradeLatency    prometheus.Histogram
	publishMatchEvents     prometheus.Histogram
	publishTradeEvents     prometheus.Histogram
	eventRelayBatchSize    prometheus.Histogram
	eventRelayDispatchMs   prometheus.Histogram
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
			Name: "matching_wal_append_latency_ms",
			Help: "WAL durable cost per command in ms: each append+fsync when sync_every<=1; amortized sync/batch_size when group commit",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500},
		}),
		walSyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_wal_sync_latency_ms",
			Help:    "WAL group commit: single fdatasync wall time per CommitBatch (not per command)",
			Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
		walSyncBatchRecords: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_wal_sync_batch_records",
			Help:    "WAL group commit: number of records included in each Sync batch",
			Buckets: []float64{1, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128},
		}),
		eventOutboxSyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_event_outbox_sync_latency_ms",
			Help:    "Event outbox fdatasync wall time per ProcessBatch",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100},
		}),
		publishLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_publish_latency_ms",
			Help:    "Kafka publish wall time per command: max(match,trade) when both topics published in parallel, else single batch",
			Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
		}),
		publishMatchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_publish_match_latency_ms",
			Help:    "Kafka publish latency for match.events batch per command",
			Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
		publishTradeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_publish_trade_latency_ms",
			Help:    "Kafka publish latency for trade.events batch per command",
			Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
		publishMatchEvents: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_publish_match_events",
			Help:    "Number of match events published per command",
			Buckets: []float64{1, 2, 3, 5, 8, 13, 21, 34},
		}),
		publishTradeEvents: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_publish_trade_events",
			Help:    "Number of trade events published per command",
			Buckets: []float64{0, 1, 2, 3, 5, 8, 13, 21, 34},
		}),
		eventRelayBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_event_relay_batch_size",
			Help:    "Event outbox relay claimed batch size per poll",
			Buckets: []float64{1, 8, 16, 32, 64, 128, 256, 512},
		}),
		eventRelayDispatchMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "matching_event_relay_dispatch_latency_ms",
			Help:    "Event outbox relay dispatch latency in milliseconds",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
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
	registerGaugeUint64("matching_event_outbox_pending_count", "Unpublished event outbox records", &m.EventOutboxPending)
	registerCounter("matching_event_relay_published_total", "Event outbox rows published to Kafka by relay", &m.EventRelayPublished)
	prometheus.MustRegister(
		m.processingLatency,
		m.walAppendLatency,
		m.walSyncLatency,
		m.walSyncBatchRecords,
		m.eventOutboxSyncLatency,
		m.publishLatency,
		m.publishMatchLatency,
		m.publishTradeLatency,
		m.publishMatchEvents,
		m.publishTradeEvents,
		m.eventRelayBatchSize,
		m.eventRelayDispatchMs,
	)
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

// ObservePublish 记录 Kafka 发布耗时与事件条数（match/trade 分 topic）。
// publish_latency 取 max(matchDur, tradeDur)，与并行 WriteBatch 的墙钟一致。
func (m *Metrics) ObservePublish(matchDur, tradeDur time.Duration, matchEvents, tradeEvents int) {
	if m == nil {
		return
	}
	wall := matchDur
	if tradeDur > wall {
		wall = tradeDur
	}
	if wall > 0 {
		m.publishLatency.Observe(float64(wall.Milliseconds()))
	}
	if matchEvents > 0 && matchDur > 0 {
		m.publishMatchLatency.Observe(float64(matchDur.Milliseconds()))
		m.publishMatchEvents.Observe(float64(matchEvents))
	}
	if tradeEvents > 0 && tradeDur > 0 {
		m.publishTradeLatency.Observe(float64(tradeDur.Milliseconds()))
		m.publishTradeEvents.Observe(float64(tradeEvents))
	}
}

// ObserveWalAppend 记录单条命令 WAL 落盘耗时（每条 append+fsync 模式）。
func (m *Metrics) ObserveWalAppend(d time.Duration) {
	if m == nil || d <= 0 {
		return
	}
	m.walAppendLatency.Observe(float64(d.Milliseconds()))
}

// ObserveWalGroupCommit 记录组提交一次 Sync：批墙钟 + 批大小，并按条摊销写入 wal_append。
func (m *Metrics) ObserveWalGroupCommit(syncDur time.Duration, records int) {
	if m == nil || records <= 0 || syncDur <= 0 {
		return
	}
	ms := float64(syncDur.Milliseconds())
	m.walSyncLatency.Observe(ms)
	m.walSyncBatchRecords.Observe(float64(records))
	perCmd := syncDur / time.Duration(records)
	m.walAppendLatency.Observe(float64(perCmd.Milliseconds()))
}

// ObserveCommandFailed 处理失败计数。
func (m *Metrics) ObserveCommandFailed() {
	if m != nil {
		m.CommandsFailed.Add(1)
	}
}

// ObserveEventOutboxSync 记录 Event Outbox 批 sync 墙钟。
func (m *Metrics) ObserveEventOutboxSync(d time.Duration) {
	if m == nil || d <= 0 {
		return
	}
	m.eventOutboxSyncLatency.Observe(float64(d.Milliseconds()))
}

// SetEventOutboxPending 更新未发布 outbox 估计值。
func (m *Metrics) SetEventOutboxPending(n uint64) {
	if m != nil {
		m.EventOutboxPending.Store(n)
	}
}

// ObserveRelayBatchSize 记录 relay 批大小。
func (m *Metrics) ObserveRelayBatchSize(n int) {
	if m != nil && n > 0 {
		m.eventRelayBatchSize.Observe(float64(n))
	}
}

// ObserveRelayDispatchLatency 记录 relay 投递耗时。
func (m *Metrics) ObserveRelayDispatchLatency(d time.Duration) {
	if m != nil && d > 0 {
		m.eventRelayDispatchMs.Observe(float64(d.Milliseconds()))
	}
}

// AddRelayPublished 累加 relay 已发布条数。
func (m *Metrics) AddRelayPublished(n int) {
	if m != nil && n > 0 {
		m.EventRelayPublished.Add(uint64(n))
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
