package metrics

import "sync/atomic"

// Counters Kline Service 运行时计数（供 Prometheus 与周期日志）。
type Counters struct {
	TradeEvents           atomic.Uint64
	OpenBarUpdates        atomic.Uint64
	ClosedBarsPersisted   atomic.Uint64
	KlineRawPublished     atomic.Uint64
	RedisPublishErrors    atomic.Uint64
	KafkaPublishErrors    atomic.Uint64
	CloseWorkerQueueFull  atomic.Uint64
	ClosedPersistFailures atomic.Uint64
}

// Snapshot 当前计数快照。
type Snapshot struct {
	TradeEvents           uint64
	OpenBarUpdates        uint64
	ClosedBarsPersisted   uint64
	KlineRawPublished     uint64
	RedisPublishErrors    uint64
	KafkaPublishErrors    uint64
	CloseWorkerQueueFull  uint64
	ClosedPersistFailures uint64
}

func (c *Counters) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	return Snapshot{
		TradeEvents:           c.TradeEvents.Load(),
		OpenBarUpdates:        c.OpenBarUpdates.Load(),
		ClosedBarsPersisted:   c.ClosedBarsPersisted.Load(),
		KlineRawPublished:     c.KlineRawPublished.Load(),
		RedisPublishErrors:    c.RedisPublishErrors.Load(),
		KafkaPublishErrors:    c.KafkaPublishErrors.Load(),
		CloseWorkerQueueFull:  c.CloseWorkerQueueFull.Load(),
		ClosedPersistFailures: c.ClosedPersistFailures.Load(),
	}
}
