package consumer

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// TradeStore 应用 trade.events 的持久化接口。
type TradeStore interface {
	ApplyTradeEvent(ctx context.Context, in repository.TradeEventApply) error
}

// TradeHandler 消费 trade.events 并写 trades、结算余额。
type TradeHandler struct {
	Repo TradeStore
}

// Process 解码并应用 TradeEvent。
func (h *TradeHandler) Process(ctx context.Context, msg kafka.Message) error {
	if h == nil || h.Repo == nil {
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

	return h.Repo.ApplyTradeEvent(ctx, repository.TradeEventApply{
		TradeID:      tr.GetTradeId(),
		Symbol:       tr.GetSymbol(),
		Price:        tr.GetPrice().GetValue(),
		Quantity:     tr.GetQuantity().GetValue(),
		MakerOrderID: tr.GetMakerOrderId(),
		TakerOrderID: tr.GetTakerOrderId(),
		WalSeq:       ev.GetWalSeq(),
	})
}
