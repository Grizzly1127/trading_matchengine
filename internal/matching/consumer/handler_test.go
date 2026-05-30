package consumer_test

import (
	"context"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	"google.golang.org/protobuf/proto"
)

func testRecoveryConfig(dir string) recovery.Config {
	reg, _ := symbolrules.DefaultRegistry()
	return recovery.Config{
		ShardID:        "shard-0",
		DataDir:        dir,
		SnapshotEvery:  1000,
		SymbolRegistry: reg,
	}
}

func TestHandler_newOrderPublishesEvents(t *testing.T) {
	dir := t.TempDir()
	eng, err := recovery.Open(testRecoveryConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	pub := publisher.NewMemory()
	h := &consumer.Handler{Engine: eng, Publisher: pub, Partition: 0}

	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID:  1,
		Symbol:   "BTC-USDT",
		Side:     engine.SideSell,
		Type:     engine.OrderTypeLimit,
		Price:    recovery.MustDecimal("100"),
		Quantity: recovery.MustDecimal("1"),
	}, 1)
	env := &matchingv1.OrderCommandEnvelope{
		Command: &matchingv1.OrderCommandEnvelope_NewOrder{NewOrder: cmd},
	}
	payload, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}

	msg := kafka.Message{Topic: "order.commands", Partition: 0, Offset: 100, Value: payload}
	if err := h.Process(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(pub.MatchEvents()) == 0 {
		t.Fatal("expected match events published")
	}
	if eng.LastSeq() != 1 {
		t.Fatalf("last_seq = %d, want 1", eng.LastSeq())
	}
}

func TestHandler_duplicateDoesNotPublish(t *testing.T) {
	dir := t.TempDir()
	eng, err := recovery.Open(testRecoveryConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	pub := publisher.NewMemory()
	h := &consumer.Handler{Engine: eng, Publisher: pub, Partition: 0}

	cmd := recovery.NewOrderFromEngine(engine.Order{
		OrderID: 2, Symbol: "BTC-USDT", Side: engine.SideBuy, Type: engine.OrderTypeLimit,
		Price: recovery.MustDecimal("50"), Quantity: recovery.MustDecimal("1"),
	}, 2)
	env := &matchingv1.OrderCommandEnvelope{
		Command: &matchingv1.OrderCommandEnvelope_NewOrder{NewOrder: cmd},
	}
	payload, _ := proto.Marshal(env)
	if err := h.Process(context.Background(), kafka.Message{Partition: 0, Offset: 1, Value: payload}); err != nil {
		t.Fatal(err)
	}
	pub.Reset()
	if err := h.Process(context.Background(), kafka.Message{Partition: 0, Offset: 2, Value: payload}); err != nil {
		t.Fatal(err)
	}
	if len(pub.MatchEvents()) != 0 {
		t.Fatalf("duplicate should not publish, got %d events", len(pub.MatchEvents()))
	}
}
