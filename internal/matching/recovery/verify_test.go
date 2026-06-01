package recovery_test

import (
	"context"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

type fakeQuerier struct {
	bySymbol map[string][]recovery.ReconcileOrder
	statuses map[uint64]string
}

func (f *fakeQuerier) ListReconcileOrders(_ context.Context, symbol string) ([]recovery.ReconcileOrder, error) {
	return f.bySymbol[symbol], nil
}

func (f *fakeQuerier) GetOrderStatuses(_ context.Context, _ string, orderIDs []uint64) (map[uint64]string, error) {
	out := make(map[uint64]string, len(orderIDs))
	for _, id := range orderIDs {
		if st, ok := f.statuses[id]; ok {
			out[id] = st
		}
	}
	return out, nil
}

func openTestEngine(t *testing.T) *recovery.Engine {
	t.Helper()
	dir := t.TempDir()
	reg, _ := symbolrules.DefaultRegistry()
	eng, err := recovery.Open(recovery.Config{
		ShardID: "shard-0", DataDir: dir, SnapshotEvery: 1000, SymbolRegistry: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

func TestVerifyAll_consistent(t *testing.T) {
	eng := openTestEngine(t)
	defer eng.Close()

	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID: 10, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("100"), Quantity: recovery.MustDecimal("1"),
	}, 1)
	if _, err := eng.ApplyNewOrder(cmd); err != nil {
		t.Fatal(err)
	}

	q := &fakeQuerier{
		bySymbol: map[string][]recovery.ReconcileOrder{
			"BTC-USDT": {{OrderID: 10, Status: status.Partial}},
		},
		statuses: map[uint64]string{10: status.Partial},
	}
	diffs, err := recovery.VerifyAll(context.Background(), eng, q, []string{"BTC-USDT"}, recovery.VerifyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("diffs=%+v want none", diffs)
	}
}

func TestVerifyAll_partialMissingInBook(t *testing.T) {
	eng := openTestEngine(t)
	defer eng.Close()

	q := &fakeQuerier{
		bySymbol: map[string][]recovery.ReconcileOrder{
			"BTC-USDT": {{OrderID: 20, Status: status.Partial}},
		},
	}
	diffs, err := recovery.VerifyAll(context.Background(), eng, q, []string{"BTC-USDT"}, recovery.VerifyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 || len(diffs[0].OnlyInDB) != 1 || diffs[0].OnlyInDB[0] != 20 {
		t.Fatalf("diffs=%+v", diffs)
	}
}

func TestVerifyAll_orphanInBook(t *testing.T) {
	eng := openTestEngine(t)
	defer eng.Close()

	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID: 30, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("100"), Quantity: recovery.MustDecimal("1"),
	}, 1)
	if _, err := eng.ApplyNewOrder(cmd); err != nil {
		t.Fatal(err)
	}

	q := &fakeQuerier{
		bySymbol: map[string][]recovery.ReconcileOrder{},
		statuses: map[uint64]string{30: status.Filled},
	}
	diffs, err := recovery.VerifyAll(context.Background(), eng, q, []string{"BTC-USDT"}, recovery.VerifyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 || len(diffs[0].OnlyInBook) != 1 {
		t.Fatalf("diffs=%+v", diffs)
	}
}

func TestVerifyAll_pendingNotSeenOK(t *testing.T) {
	eng := openTestEngine(t)
	defer eng.Close()

	q := &fakeQuerier{
		bySymbol: map[string][]recovery.ReconcileOrder{
			"BTC-USDT": {{OrderID: 40, Status: status.Pending}},
		},
	}
	diffs, err := recovery.VerifyAll(context.Background(), eng, q, []string{"BTC-USDT"}, recovery.VerifyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("pending not in WAL should be ok, diffs=%+v", diffs)
	}
}
