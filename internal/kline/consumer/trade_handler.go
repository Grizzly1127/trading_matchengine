package consumer

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/publisher"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
	"github.com/Grizzly1127/trading_matchengine/pkg/tradeevent"
)

// TradeHandler 消费 trade.events 并聚合 K 线。
type TradeHandler struct {
	Aggregator *aggregator.Aggregator
	Publisher  *publisher.RedisPublisher
}

// Process 解码成交并更新聚合器；同步刷新 Redis open 快照与推送。
func (h *TradeHandler) Process(ctx context.Context, msg kafka.Message) error {
	if h == nil || h.Aggregator == nil || h.Publisher == nil {
		return fmt.Errorf("kline trade handler: not configured")
	}
	tr, err := tradeevent.ParseKafkaMessage(msg)
	if err != nil {
		return fmt.Errorf("kline trade handler: %w", err)
	}
	if err := h.Aggregator.ApplyTrade(tr.Symbol, tr.Price, tr.Quantity, tr.TradeTimeMs); err != nil {
		return fmt.Errorf("kline trade handler: apply: %w", err)
	}
	return h.publishOpenBars(ctx, tr.Symbol)
}

func (h *TradeHandler) publishOpenBars(ctx context.Context, symbol string) error {
	for _, iv := range interval.DefaultIntervals {
		b, ok := h.Aggregator.SnapshotOpen(symbol, iv)
		if !ok {
			continue
		}
		if err := h.Publisher.SaveOpenBar(ctx, symbol, iv, b); err != nil {
			return fmt.Errorf("save open bar %s %s: %w", symbol, iv, err)
		}
		if err := h.Publisher.PublishOpenUpdate(ctx, symbol, iv, b); err != nil {
			return fmt.Errorf("publish open bar %s %s: %w", symbol, iv, err)
		}
	}
	return nil
}
