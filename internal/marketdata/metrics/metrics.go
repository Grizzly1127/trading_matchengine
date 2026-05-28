package metrics

import "sync/atomic"

type Counters struct {
	TradeEvents        atomic.Uint64
	MatchEvents        atomic.Uint64
	DepthPublished     atomic.Uint64
	TickerAllPublished atomic.Uint64
	RedisPublishErrors atomic.Uint64
}

type Snapshot struct {
	TradeEvents        uint64
	MatchEvents        uint64
	DepthPublished     uint64
	TickerAllPublished uint64
	RedisPublishErrors uint64
}

func (c *Counters) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	return Snapshot{
		TradeEvents:        c.TradeEvents.Load(),
		MatchEvents:        c.MatchEvents.Load(),
		DepthPublished:     c.DepthPublished.Load(),
		TickerAllPublished: c.TickerAllPublished.Load(),
		RedisPublishErrors: c.RedisPublishErrors.Load(),
	}
}
