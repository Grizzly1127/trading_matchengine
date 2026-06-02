package convert

import (
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTradeFromPB(t *testing.T) {
	ts := timestamppb.Now()
	got := TradeFromPB(&orderv1.TradeInfo{
		TradeId:  99,
		Symbol:   "BTC-USDT",
		Price:    &commonv1.Decimal{Value: "100.5"},
		Quantity: &commonv1.Decimal{Value: "0.1"},
		OrderId:  7,
		Side:     "SELL",
		IsMaker:  true,
		CreatedAt: ts,
	})
	if got.TradeID != "99" || got.OrderID != "7" || got.Side != "SELL" || !got.IsMaker {
		t.Fatalf("got=%+v", got)
	}
	if got.Fee != "0" || got.TradedAt == "" {
		t.Fatalf("fee/traded_at: %+v", got)
	}
}
