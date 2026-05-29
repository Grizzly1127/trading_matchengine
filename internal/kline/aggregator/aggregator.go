package aggregator

import (
	"fmt"
	"sync"

	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
)

// ClosedEvent 时间桶切换产生的已闭合 bar。
type ClosedEvent struct {
	Symbol   string
	Interval interval.Interval
	Bar      bar.OHLCV
}

// Aggregator 按 symbol + interval 在内存维护当前 open bar。
type Aggregator struct {
	mu        sync.RWMutex
	symbols   map[string]*symbolState
	onClose   func(ClosedEvent)
	intervals []interval.Interval
}

type symbolState struct {
	mu   sync.Mutex
	bars map[interval.Interval]bar.OHLCV
}

// DefaultIntervals 返回默认聚合周期列表。
func DefaultIntervals() []interval.Interval {
	return interval.DefaultIntervals
}

// NewAggregator 创建聚合器。
func NewAggregator() *Aggregator {
	return &Aggregator{
		symbols:   make(map[string]*symbolState),
		intervals: interval.DefaultIntervals,
	}
}

// SetOnClose 注册闭合回调（应在启动消费前设置）。
func (a *Aggregator) SetOnClose(fn func(ClosedEvent)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onClose = fn
}

// RestoreOpen 从 Redis 恢复未闭合 bar（启动时调用）。
func (a *Aggregator) RestoreOpen(symbol string, iv interval.Interval, b bar.OHLCV) {
	st := a.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.bars[iv] = b
}

// SnapshotOpen 读取当前未闭合 bar。
func (a *Aggregator) SnapshotOpen(symbol string, iv interval.Interval) (bar.OHLCV, bool) {
	st := a.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()
	b, ok := st.bars[iv]
	return b, ok
}

// ApplyTrade 应用一笔成交，可能触发 onClose。
func (a *Aggregator) ApplyTrade(symbol string, priceStr, qtyStr string, tradeTimeMs int64) error {
	if symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	price, err := bar.ParseDecimal(priceStr)
	if err != nil {
		return err
	}
	qty, err := bar.ParseDecimal(qtyStr)
	if err != nil {
		return err
	}

	var closed []ClosedEvent
	for _, iv := range a.intervals {
		if ev, ok := a.applyOneInterval(symbol, iv, price, qty, tradeTimeMs); ok {
			closed = append(closed, ev)
		}
	}
	if len(closed) == 0 {
		return nil
	}
	fn := a.getOnClose()
	if fn == nil {
		return nil
	}
	for _, ev := range closed {
		fn(ev)
	}
	return nil
}

func (a *Aggregator) getOnClose() func(ClosedEvent) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.onClose
}

func (a *Aggregator) getOrCreate(symbol string) *symbolState {
	a.mu.RLock()
	st := a.symbols[symbol]
	a.mu.RUnlock()
	if st != nil {
		return st
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if st = a.symbols[symbol]; st != nil {
		return st
	}
	st = &symbolState{bars: make(map[interval.Interval]bar.OHLCV)}
	a.symbols[symbol] = st
	return st
}

func (a *Aggregator) applyOneInterval(symbol string, iv interval.Interval, price, qty decimal.Decimal, tradeTimeMs int64) (ClosedEvent, bool) {
	bucketStart := iv.BucketStartMs(tradeTimeMs)
	st := a.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()

	cur, ok := st.bars[iv]
	if !ok {
		st.bars[iv] = bar.NewFromTrade(bucketStart, price, qty, tradeTimeMs)
		return ClosedEvent{}, false
	}

	if tradeTimeMs < cur.OpenTimeMs {
		return ClosedEvent{}, false
	}

	if bucketStart == cur.OpenTimeMs {
		cur.ApplyTrade(price, qty, tradeTimeMs)
		st.bars[iv] = cur
		return ClosedEvent{}, false
	}

	closed := ClosedEvent{
		Symbol:   symbol,
		Interval: iv,
		Bar:      cur.Clone(),
	}
	st.bars[iv] = bar.NewFromTrade(bucketStart, price, qty, tradeTimeMs)
	return closed, true
}
