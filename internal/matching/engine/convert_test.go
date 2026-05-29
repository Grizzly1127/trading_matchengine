package engine

import (
	"testing"

	"github.com/shopspring/decimal"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

func TestOrderFromProto_OrderToProto_roundTrip(t *testing.T) {
	pb := &commonv1.Order{
		OrderId:       1000000001,
		ClientOrderId: "client-1",
		Symbol:        "BTC-USDT",
		Side:          commonv1.Side_SIDE_BUY,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:         &commonv1.Decimal{Value: "65000.50"},
		Quantity:      &commonv1.Decimal{Value: "0.01"},
		Flags:         0,
	}

	got, err := OrderFromProto(pb)
	if err != nil {
		t.Fatal(err)
	}
	if got.OrderID != 1000000001 || got.Symbol != "BTC-USDT" {
		t.Fatalf("got = %+v", got)
	}
	wantPrice, _ := decimal.NewFromString("65000.50")
	if !got.Price.Equal(wantPrice) {
		t.Fatalf("price = %s, want %s", got.Price, wantPrice)
	}

	back := OrderToProto(got)
	if back.GetOrderId() != pb.GetOrderId() || back.GetClientOrderId() != pb.GetClientOrderId() {
		t.Fatalf("back = %+v", back)
	}
}

func TestOrderFromProto_rejectsUnspecifiedSide(t *testing.T) {
	_, err := OrderFromProto(&commonv1.Order{
		OrderId:  1,
		Symbol:   "BTC-USDT",
		Side:     commonv1.Side_SIDE_UNSPECIFIED,
		Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:    &commonv1.Decimal{Value: "1"},
		Quantity: &commonv1.Decimal{Value: "1"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTradeFromProto_TradeToProto_roundTrip(t *testing.T) {
	pb := &commonv1.Trade{
		TradeId:      9001,
		Symbol:       "BTC-USDT",
		Price:        &commonv1.Decimal{Value: "100.5"},
		Quantity:     &commonv1.Decimal{Value: "0.25"},
		MakerOrderId: 1,
		TakerOrderId: 2,
	}

	got, err := TradeFromProto(pb)
	if err != nil {
		t.Fatal(err)
	}
	wantPrice, _ := decimal.NewFromString("100.5")
	wantQty, _ := decimal.NewFromString("0.25")
	if got.TradeID != 9001 || !got.Price.Equal(wantPrice) || !got.Quantity.Equal(wantQty) {
		t.Fatalf("got = %+v", got)
	}

	back := TradeToProto(got)
	if back.GetTradeId() != pb.GetTradeId() || back.GetMakerOrderId() != pb.GetMakerOrderId() {
		t.Fatalf("back = %+v", back)
	}
}

func TestOrderFromProto_withRemaining(t *testing.T) {
	got, err := OrderFromProto(&commonv1.Order{
		OrderId:   2,
		Symbol:    "BTC-USDT",
		Side:      commonv1.Side_SIDE_SELL,
		Type:      commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price:     &commonv1.Decimal{Value: "100"},
		Quantity:  &commonv1.Decimal{Value: "10"},
		Remaining: &commonv1.Decimal{Value: "3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantRemaining, _ := decimal.NewFromString("3")
	if !got.Remaining.Equal(wantRemaining) {
		t.Fatalf("remaining = %s, want %s", got.Remaining, wantRemaining)
	}
}

func TestOrderFromProto_rejectsInvalidOrderType(t *testing.T) {
	_, err := OrderFromProto(&commonv1.Order{
		OrderId:  1,
		Symbol:   "BTC-USDT",
		Side:     commonv1.Side_SIDE_BUY,
		Type:     commonv1.OrderType_ORDER_TYPE_UNSPECIFIED,
		Price:    &commonv1.Decimal{Value: "1"},
		Quantity: &commonv1.Decimal{Value: "1"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOrderFromProto_rejectsMissingPrice(t *testing.T) {
	_, err := OrderFromProto(&commonv1.Order{
		OrderId:  1,
		Symbol:   "BTC-USDT",
		Side:     commonv1.Side_SIDE_BUY,
		Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
		Quantity: &commonv1.Decimal{Value: "1"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOrderFromProto_marketWithoutPrice(t *testing.T) {
	got, err := OrderFromProto(&commonv1.Order{
		OrderId:  2,
		Symbol:   "BTC-USDT",
		Side:     commonv1.Side_SIDE_BUY,
		Type:     commonv1.OrderType_ORDER_TYPE_MARKET,
		Quantity: &commonv1.Decimal{Value: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != OrderTypeMarket || !got.Price.IsZero() {
		t.Fatalf("got = %+v", got)
	}
}

func TestOrderFromProto_rejectsNil(t *testing.T) {
	if _, err := OrderFromProto(nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestTradeFromProto_rejectsNil(t *testing.T) {
	if _, err := TradeFromProto(nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestOrderProtoRoundTrip_preservesMatchFields(t *testing.T) {
	want := Order{
		OrderID: 1, Symbol: "BTC-USDT", Side: SideSell, Type: OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	}
	got, err := OrderFromProto(OrderToProto(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.Side != want.Side || got.Type != want.Type ||
		!got.Price.Equal(want.Price) || !got.Quantity.Equal(want.Quantity) {
		t.Fatalf("got = %+v want = %+v", got, want)
	}

	book := NewOrderBook("BTC-USDT")
	if _, err := book.Match(got, 1); err != nil {
		t.Fatal(err)
	}
	buy, err := OrderFromProto(OrderToProto(Order{
		OrderID: 2, Symbol: "BTC-USDT", Side: SideBuy, Type: OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	trades, err := book.Match(buy, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || book.ActiveOrderCount() != 0 {
		t.Fatalf("trades=%d active=%d", len(trades), book.ActiveOrderCount())
	}
}

func TestOrderToProto_marketAndSell(t *testing.T) {
	pb := OrderToProto(Order{
		OrderID:  5,
		Symbol:   "BTC-USDT",
		Side:     SideSell,
		Type:     OrderTypeMarket,
		Price:    decimal.NewFromInt(100),
		Quantity: decimal.NewFromInt(1),
	})
	if pb.GetSide() != commonv1.Side_SIDE_SELL || pb.GetType() != commonv1.OrderType_ORDER_TYPE_MARKET {
		t.Fatalf("pb = %+v", pb)
	}
}
