package publisher_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/symbol"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

func testShard(t *testing.T) *symbol.Shard {
	t.Helper()
	sh := symbol.NewShard()
	reg, err := symbolrules.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	symbol.RegisterRegistry(sh, reg)
	return sh
}

func TestBuildNewOrderEvents_restingOrderAccepted(t *testing.T) {
	shard := testShard(t)
	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID:  1,
		Symbol:   "BTC-USDT",
		Side:     engine.SideSell,
		Type:     engine.OrderTypeLimit,
		Price:    recovery.MustDecimal("100"),
		Quantity: recovery.MustDecimal("1"),
	}, 1)

	out := publisher.BuildNewOrderEvents(shard, cmd, nil, 10, false)
	if len(out.TradeEvents) != 0 {
		t.Fatalf("trade events = %d", len(out.TradeEvents))
	}
	if len(out.MatchEvents) != 1 {
		t.Fatalf("match events = %d, want 1", len(out.MatchEvents))
	}
	if out.MatchEvents[0].GetEventType() != matchingv1.MatchEventType_ORDER_ACCEPTED {
		t.Fatalf("event type = %v", out.MatchEvents[0].GetEventType())
	}
}

func TestBuildNewOrderEvents_fullMatchEmitsTradeAndFilled(t *testing.T) {
	shard := testShard(t)
	sell := engine.Order{
		OrderID: 10, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("100"), Quantity: recovery.MustDecimal("1"),
	}
	buy := engine.Order{
		OrderID: 11, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("100"), Quantity: recovery.MustDecimal("1"),
	}
	if _, err := shard.Match(sell, 1); err != nil {
		t.Fatal(err)
	}
	trades, err := shard.Match(buy, 2)
	if err != nil {
		t.Fatal(err)
	}
	cmd := recovery.NewOrderFromEngine(buy, 2)

	out := publisher.BuildNewOrderEvents(shard, cmd, trades, 20, false)
	if len(out.TradeEvents) != 1 {
		t.Fatalf("trade events = %d, want 1", len(out.TradeEvents))
	}
	types := make(map[matchingv1.MatchEventType]int)
	for _, ev := range out.MatchEvents {
		types[ev.GetEventType()]++
	}
	if types[matchingv1.MatchEventType_ORDER_ACCEPTED] != 1 {
		t.Fatalf("accepted count = %d", types[matchingv1.MatchEventType_ORDER_ACCEPTED])
	}
	if types[matchingv1.MatchEventType_ORDER_FILLED] < 2 {
		t.Fatalf("filled events = %+v", types)
	}
}

func TestBuildNewOrderEvents_duplicateIsEmpty(t *testing.T) {
	shard := testShard(t)
	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID: 99, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("1"), Quantity: recovery.MustDecimal("1"),
	}, 99)

	out := publisher.BuildNewOrderEvents(shard, cmd, nil, 5, true)
	if len(out.MatchEvents) != 0 || len(out.TradeEvents) != 0 {
		t.Fatalf("expected empty outbound, got %+v", out)
	}
}

func TestBuildCancelEvents_emitsCanceled(t *testing.T) {
	cmd := &matchingv1.CancelOrderCommand{CommandId: 7, Symbol: "BTC-USDT", OrderId: 30}
	out := publisher.BuildCancelEvents(cmd, 8)
	if len(out.MatchEvents) != 1 {
		t.Fatalf("match events = %d", len(out.MatchEvents))
	}
	if out.MatchEvents[0].GetEventType() != matchingv1.MatchEventType_ORDER_CANCELED {
		t.Fatalf("type = %v", out.MatchEvents[0].GetEventType())
	}
}
