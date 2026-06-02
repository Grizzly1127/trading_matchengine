package tickerallpub

import (
	"context"
	"sync"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/tickerall"
)

// Publisher 按间隔 diff 并发布 ticker@all WS 帧；REST 快照写入 Redis。
type Publisher struct {
	Store    *store.Store
	Redis    *publisher.RedisPublisher
	Interval time.Duration
	// HeartbeatEvery 为 0 时默认 60s。
	HeartbeatEvery time.Duration
	QuoteAssets    []string

	mu             sync.Mutex
	lastItems      map[string]map[string]tickerall.CompactItem
	lastHeartbeat map[string]time.Time
	OnPublished   func()
}

func (p *Publisher) Run(ctx context.Context) {
	if p == nil || p.Store == nil || p.Redis == nil {
		return
	}
	interval := p.Interval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	hb := p.HeartbeatEvery
	if hb <= 0 {
		hb = 60 * time.Second
	}
	quotes := append([]string(nil), p.QuoteAssets...)
	if len(quotes) == 0 {
		quotes = []string{"USDT"}
	}

	p.mu.Lock()
	if p.lastItems == nil {
		p.lastItems = make(map[string]map[string]tickerall.CompactItem)
	}
	if p.lastHeartbeat == nil {
		p.lastHeartbeat = make(map[string]time.Time)
	}
	p.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for _, quote := range quotes {
				p.tick(ctx, quote, now, hb)
			}
		}
	}
}

func (p *Publisher) tick(ctx context.Context, quoteAsset string, now time.Time, hb time.Duration) {
	snap := p.Store.BuildTickerAllSnapshot(quoteAsset)
	stream := tickerall.WSStream(quoteAsset)
	ch := tickerall.PubSubChannel(quoteAsset)
	ts := now.UnixMilli()
	if snap.SnapshotTime > 0 {
		ts = snap.SnapshotTime
	}

	if err := p.Redis.SetTickerAllREST(ctx, snap); err != nil {
		return
	}

	curr := compactMapFromStore(snap)
	qk := tickerall.NormalizeQuoteAsset(quoteAsset)

	p.mu.Lock()
	prev := p.lastItems[qk]
	needHB := now.Sub(p.lastHeartbeat[qk]) >= hb
	p.mu.Unlock()

	if len(prev) == 0 {
		items := tickerall.ItemsFromCompactMap(curr)
		payload, err := tickerall.MarshalSnapshot(stream, snap.SnapshotID, ts, snap.Count, items)
		if err != nil {
			return
		}
		if err := p.Redis.PublishTickerAllWS(ctx, ch, payload); err != nil {
			return
		}
		p.mu.Lock()
		p.lastItems[qk] = cloneMap(curr)
		p.lastHeartbeat[qk] = now
		p.mu.Unlock()
		p.countPublished()
		return
	}

	delta := tickerall.Diff(prev, curr)
	if len(delta) > 0 {
		payload, err := tickerall.MarshalDelta(stream, snap.SnapshotID, ts, delta)
		if err != nil {
			return
		}
		if err := p.Redis.PublishTickerAllWS(ctx, ch, payload); err != nil {
			return
		}
		p.mu.Lock()
		p.lastItems[qk] = cloneMap(curr)
		p.mu.Unlock()
		p.countPublished()
	}

	if needHB {
		payload, err := tickerall.MarshalHeartbeat(stream, snap.SnapshotID, ts)
		if err != nil {
			return
		}
		_ = p.Redis.PublishTickerAllWS(ctx, ch, payload)
		p.mu.Lock()
		p.lastHeartbeat[qk] = now
		p.mu.Unlock()
		p.countPublished()
	}
}

func (p *Publisher) countPublished() {
	if p.OnPublished != nil {
		p.OnPublished()
	}
}

func compactMapFromStore(snap store.TickerAllSnapshot) map[string]tickerall.CompactItem {
	out := make(map[string]tickerall.CompactItem, len(snap.Items))
	for _, it := range snap.Items {
		out[it.Symbol] = tickerall.CompactItemFromFields(
			it.Symbol,
			store.FormatDecimal(it.LastPrice),
			store.FormatDecimal(it.Volume),
			store.FormatDecimal(it.QuoteVolume),
			store.FormatPercent(it.PriceChangePercent),
		)
	}
	return out
}

func cloneMap(m map[string]tickerall.CompactItem) map[string]tickerall.CompactItem {
	out := make(map[string]tickerall.CompactItem, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
