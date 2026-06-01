package recovery_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/presence"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

func TestEngineLookupOrderPresence(t *testing.T) {
	dir := t.TempDir()
	reg, _ := symbolrules.DefaultRegistry()
	eng, err := recovery.Open(recovery.Config{
		ShardID: "shard-0", DataDir: dir, SnapshotEvery: 1000, SymbolRegistry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	if got := eng.LookupOrderPresence("BTC-USDT", 99); got != presence.Unknown {
		t.Fatalf("unknown order presence=%v", got)
	}

	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID: 10, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("100"), Quantity: recovery.MustDecimal("1"),
	}, 1)
	if _, err := eng.ApplyNewOrder(cmd); err != nil {
		t.Fatal(err)
	}
	if got := eng.LookupOrderPresence("BTC-USDT", 10); got != presence.InOrderbook {
		t.Fatalf("resting presence=%v want in_orderbook", got)
	}

	if err := eng.ApplyCancel(&matchingv1.CancelOrderCommand{
		CommandId: 2, Symbol: "BTC-USDT", OrderId: 10,
	}); err != nil {
		t.Fatal(err)
	}
	if got := eng.LookupOrderPresence("BTC-USDT", 10); got != presence.KnownNotInOrderbook {
		t.Fatalf("after cancel presence=%v", got)
	}
}
