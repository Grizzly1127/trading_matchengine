package engine_test

import (
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/shopspring/decimal"
)

func TestOrderBook_ExportRestore_roundTrip(t *testing.T) {
	book := engine.NewOrderBook("BTC-USDT")

	sell := engine.Order{
		OrderID: 1, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(2),
	}
	buy := engine.Order{
		OrderID: 2, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(99), Quantity: decimal.NewFromInt(1),
	}
	_, _ = book.Match(sell, 1)
	_, _ = book.Match(buy, 2)

	snap := book.ExportSnapshot("shard-0", 2, time.Unix(1, 0).UTC())
	if err := book.ValidateWithOrderMap(snap.GetOrderMap()); err != nil {
		t.Fatalf("source book: %v", err)
	}

	restored := engine.NewOrderBook("BTC-USDT")
	if err := restored.RestoreFromSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := restored.ValidateWithOrderMap(snap.GetOrderMap()); err != nil {
		t.Fatal(err)
	}

	bid, ok := restored.BestBid()
	if !ok || !bid.Equal(decimal.NewFromInt(99)) {
		t.Fatalf("bestBid = %s ok=%v", bid, ok)
	}
	ask, ok := restored.BestAsk()
	if !ok || !ask.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("bestAsk = %s ok=%v", ask, ok)
	}
}

func TestOrderBook_Restore_rejectsCrossedBook(t *testing.T) {
	book := engine.NewOrderBook("BTC-USDT")
	snap := &matchingv1.Snapshot{
		Symbol: "BTC-USDT",
		Bids: []*matchingv1.PriceLevel{
			{
				Price: &commonv1.Decimal{Value: "101"},
				Orders: []*commonv1.Order{{
					OrderId: 1, Symbol: "BTC-USDT",
					Side: commonv1.Side_SIDE_BUY, Type: commonv1.OrderType_ORDER_TYPE_LIMIT,
					Price: &commonv1.Decimal{Value: "101"}, Quantity: &commonv1.Decimal{Value: "1"},
				}},
			},
		},
		Asks: []*matchingv1.PriceLevel{
			{
				Price: &commonv1.Decimal{Value: "100"},
				Orders: []*commonv1.Order{{
					OrderId: 2, Symbol: "BTC-USDT",
					Side: commonv1.Side_SIDE_SELL, Type: commonv1.OrderType_ORDER_TYPE_LIMIT,
					Price: &commonv1.Decimal{Value: "100"}, Quantity: &commonv1.Decimal{Value: "1"},
				}},
			},
		},
	}
	if err := book.RestoreFromSnapshot(snap); err != engine.ErrSpreadViolation {
		t.Fatalf("err = %v", err)
	}
}

func TestOrderBook_groupCommitScenario_orderMapConsistent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress in -short")
	}
	book := engine.NewOrderBook("BTC-USDT")
	price := decimal.NewFromInt(65000)
	qty := decimal.RequireFromString("0.001")
	for i := uint64(1); i <= 551; i++ {
		o := engine.Order{
			OrderID: i, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
			Price: price, Quantity: qty, Remaining: qty, UpdateTime: time.Now(),
		}
		if _, err := book.Match(o, i); err != nil {
			t.Fatalf("sell %d: %v", i, err)
		}
	}
	for i := uint64(552); i <= 100_000; i++ {
		o := engine.Order{
			OrderID: i, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
			Price: price, Quantity: qty, Remaining: qty, UpdateTime: time.Now(),
		}
		if _, err := book.Match(o, i); err != nil {
			t.Fatalf("buy %d: %v", i, err)
		}
	}
	snap := book.ExportSnapshot("shard-0", 100_000, time.Now())
	if err := book.ValidateWithOrderMap(snap.GetOrderMap()); err != nil {
		t.Fatalf("validate: %v map=%d active=%d", err, len(snap.GetOrderMap()), book.ActiveOrderCount())
	}
}

func TestOrderBook_manyRestingBuys_orderMapConsistent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress in -short")
	}
	book := engine.NewOrderBook("BTC-USDT")
	price := decimal.NewFromInt(65000)
	qty := decimal.RequireFromString("0.001")
	for i := uint64(1); i <= 100_000; i++ {
		o := engine.Order{
			OrderID: i, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
			Price: price, Quantity: qty, Remaining: qty, UpdateTime: time.Now(),
		}
		if _, err := book.Match(o, i); err != nil {
			t.Fatalf("match %d: %v", i, err)
		}
	}
	snap := book.ExportSnapshot("shard-0", 100_000, time.Now())
	if err := book.ValidateWithOrderMap(snap.GetOrderMap()); err != nil {
		t.Fatalf("validate: %v (map=%d active=%d)", err, len(snap.GetOrderMap()), book.ActiveOrderCount())
	}
}

func TestOrderBook_crossedMatching_orderMapConsistent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress in -short")
	}
	book := engine.NewOrderBook("BTC-USDT")
	price := decimal.NewFromInt(65000)
	qty := decimal.RequireFromString("0.001")
	for i := uint64(1); i <= 100_000; i++ {
		side := engine.SideBuy
		if i%10 < 7 { // 70% buy taker vs resting sells
			side = engine.SideBuy
		} else {
			side = engine.SideSell
		}
		o := engine.Order{
			OrderID: i, Symbol: "BTC-USDT", Side: side, Type: engine.OrderTypeLimit,
			Price: price, Quantity: qty, Remaining: qty, UpdateTime: time.Now(),
		}
		if _, err := book.Match(o, i); err != nil {
			t.Fatalf("match %d: %v", i, err)
		}
		if i%10_000 == 0 {
			snap := book.ExportSnapshot("shard-0", i, time.Now())
			if err := book.ValidateWithOrderMap(snap.GetOrderMap()); err != nil {
				t.Fatalf("at %d: %v (map=%d active=%d)", i, err, len(snap.GetOrderMap()), book.ActiveOrderCount())
			}
		}
	}
}
