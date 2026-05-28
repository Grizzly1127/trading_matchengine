package consumer

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
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
	var ev matchingv1.TradeEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("trade handler: decode: %w", err)
	}
	tr := ev.GetTrade()
	if tr == nil || tr.GetTradeId() == 0 {
		return fmt.Errorf("trade handler: trade_id is required")
	}
	if tr.GetPrice() == nil || tr.GetQuantity() == nil {
		return fmt.Errorf("trade handler: price and quantity are required")
	}

	symbol := tr.GetSymbol()
	price := tr.GetPrice().GetValue()
	qty := tr.GetQuantity().GetValue()

	updatedAtMs := time.Now().UnixMilli()
	if ct := tr.GetCreateTime(); ct != nil {
		updatedAtMs = ct.AsTime().UnixMilli()
	}

	if err := h.Store.ApplyTrade(symbol, price, qty, updatedAtMs); err != nil {
		return fmt.Errorf("trade handler: apply ticker: %w", err)
	}

	snap, ok := h.Store.SnapshotTicker(symbol)
	if !ok {
		return nil
	}
	if err := h.Publisher.PublishTicker(ctx, symbol, snap); err != nil {
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
