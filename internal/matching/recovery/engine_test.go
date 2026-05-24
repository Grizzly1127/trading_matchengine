package recovery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/shopspring/decimal"
)

func TestProtoRoundTrip_matchClearsBook(t *testing.T) {
	sell, err := engine.OrderFromProto(limitSell(1, "100", "1").GetOrder())
	if err != nil {
		t.Fatalf("sell from proto: %v", err)
	}
	buy, err := engine.OrderFromProto(limitBuy(2, "100", "1").GetOrder())
	if err != nil {
		t.Fatalf("buy from proto: %v", err)
	}
	book := engine.NewOrderBook("BTC-USDT")
	if _, err := book.Match(sell, 1); err != nil {
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

func testConfig(t *testing.T, dir string) recovery.Config {
	t.Helper()
	return recovery.Config{
		ShardID:       "shard-0",
		DataDir:       dir,
		SnapshotEvery: 1000,
	}
}

func limitSell(orderID uint64, price string, qty string) *matchingv1.NewOrderCommand {
	return recovery.NewOrderFromEngine(engine.Order{
		OrderID:  orderID,
		Symbol:   "BTC-USDT",
		Side:     engine.SideSell,
		Type:     engine.OrderTypeLimit,
		Price:    recovery.MustDecimal(price),
		Quantity: recovery.MustDecimal(qty),
	}, orderID)
}

func limitBuy(orderID uint64, price string, qty string) *matchingv1.NewOrderCommand {
	return recovery.NewOrderFromEngine(engine.Order{
		OrderID:  orderID,
		Symbol:   "BTC-USDT",
		Side:     engine.SideBuy,
		Type:     engine.OrderTypeLimit,
		Price:    recovery.MustDecimal(price),
		Quantity: recovery.MustDecimal(qty),
	}, orderID)
}

func TestEngine_restartPreservesRestingOrder(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	e1, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e1.ApplyNewOrder(limitSell(1, "100", "1")); err != nil {
		t.Fatal(err)
	}
	if err := e1.SnapshotNow(); err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	book := e2.Shard().Symbol("BTC-USDT").OrderBook
	ask, ok := book.BestAsk()
	if !ok || !ask.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("bestAsk = %s ok=%v", ask, ok)
	}
}

func TestEngine_replayDoesNotDoubleMatch(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	e1, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e1.ApplyNewOrder(limitSell(10, "100", "1")); err != nil {
		t.Fatal(err)
	}
	trades, err := e1.ApplyNewOrder(limitBuy(11, "100", "1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(trades))
	}
	if err := e1.SnapshotNow(); err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if _, ok := e2.Shard().Symbol("BTC-USDT").OrderBook.BestAsk(); ok {
		t.Fatal("asks should stay empty after replay")
	}
	if _, ok := e2.Shard().Symbol("BTC-USDT").OrderBook.BestBid(); ok {
		t.Fatal("bids should stay empty after replay")
	}
}

func TestEngine_duplicateOrderID_isIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	e, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	cmd := limitSell(20, "50", "1")
	if _, err := e.ApplyNewOrder(cmd); err != nil {
		t.Fatal(err)
	}
	trades, err := e.ApplyNewOrder(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if trades != nil {
		t.Fatalf("expected nil trades on duplicate, got %+v", trades)
	}
}

func TestEngine_cancelPersistsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	e1, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e1.ApplyNewOrder(limitBuy(30, "99", "1")); err != nil {
		t.Fatal(err)
	}
	if err := e1.ApplyCancel(&matchingv1.CancelOrderCommand{
		CommandId: 31,
		Symbol:    "BTC-USDT",
		OrderId:   30,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e1.SnapshotNow(); err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if _, ok := e2.Shard().Symbol("BTC-USDT").OrderBook.BestBid(); ok {
		t.Fatal("bid should be gone after cancel + restart")
	}
}

func TestEngine_MaxKafkaOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	e, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	cmd := limitSell(50, "100", "1")
	cmd.KafkaPartition = 0
	cmd.KafkaOffset = 42
	if _, err := e.ApplyNewOrder(cmd); err != nil {
		t.Fatal(err)
	}

	off, ok := e.MaxKafkaOffset(0)
	if !ok || off != 42 {
		t.Fatalf("max offset = %d ok=%v, want 42 true", off, ok)
	}
}

func TestEngine_walAndSnapshotFilesCreated(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)

	e, err := recovery.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ApplyNewOrder(limitSell(40, "100", "2")); err != nil {
		t.Fatal(err)
	}
	if err := e.SnapshotNow(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	walDir := filepath.Join(dir, "wal", cfg.ShardID)
	if matches, _ := filepath.Glob(filepath.Join(walDir, "wal_*.log")); len(matches) == 0 {
		t.Fatal("expected wal segment file")
	}
	snapPath := filepath.Join(dir, "snapshots", cfg.ShardID, "BTC-USDT")
	if _, err := os.Stat(filepath.Join(dir, "snapshots", cfg.ShardID, "manifest.pb")); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	matches, _ := filepath.Glob(filepath.Join(snapPath, "snapshot_*.pb"))
	if len(matches) == 0 {
		t.Fatal("expected snapshot file")
	}
}
