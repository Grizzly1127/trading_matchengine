package metrics

import "sync/atomic"

// Counters 进程内计数（后续可接 Prometheus）。
type Counters struct {
	TicksTotal         atomic.Uint64
	AggregateOK        atomic.Uint64
	AggregateFailed    atomic.Uint64
	SourceFetchErrors  atomic.Uint64
	RedisPublishErrors atomic.Uint64
	KafkaPublishErrors atomic.Uint64
	AuditWriteErrors   atomic.Uint64
}

type Snapshot struct {
	TicksTotal         uint64
	AggregateOK        uint64
	AggregateFailed    uint64
	SourceFetchErrors  uint64
	RedisPublishErrors uint64
	KafkaPublishErrors uint64
	AuditWriteErrors   uint64
}

func (c *Counters) Snap() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	return Snapshot{
		TicksTotal:         c.TicksTotal.Load(),
		AggregateOK:        c.AggregateOK.Load(),
		AggregateFailed:    c.AggregateFailed.Load(),
		SourceFetchErrors:  c.SourceFetchErrors.Load(),
		RedisPublishErrors: c.RedisPublishErrors.Load(),
		KafkaPublishErrors: c.KafkaPublishErrors.Load(),
		AuditWriteErrors:   c.AuditWriteErrors.Load(),
	}
}
