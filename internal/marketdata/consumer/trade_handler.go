package consumer

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/tradeevent"
)

// TradeHandler 消费 trade.events 并更新内存 Ticker，同时写入/推送 Redis。
type TradeHandler struct {
	Store     *store.Store
	Publisher *publisher.RedisPublisher
	Metrics   *metrics.Counters
}

// Process 解码并应用 TradeEvent。
func (h *TradeHandler) Process(ctx context.Context, msg kafka.Message) error {
	if h == nil || h.Store == nil || h.Publisher == nil {
		return fmt.Errorf("trade handler: not configured")
	}
	tr, err := tradeevent.ParseKafkaMessage(msg)
	if err != nil {
		return fmt.Errorf("trade handler: %w", err)
	}

	if err := h.Store.ApplyTrade(tr.Symbol, tr.Price, tr.Quantity, tr.TradeTimeMs); err != nil {
		return fmt.Errorf("trade handler: apply ticker: %w", err)
	}

	snap, ok := h.Store.SnapshotTicker(tr.Symbol)
	if !ok {
		return nil
	}
	if err := h.Publisher.PublishTicker(ctx, tr.Symbol, snap); err != nil {
		if h.Metrics != nil {
			h.Metrics.RedisPublishErrors.Add(1)
		}
		return fmt.Errorf("trade handler: publish ticker: %w", err)
	}
	if h.Metrics != nil {
		h.Metrics.TradeEvents.Add(1)
	}
	return nil
}
