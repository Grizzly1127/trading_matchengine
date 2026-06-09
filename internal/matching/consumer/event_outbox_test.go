package consumer_test

import (
	"context"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/eventoutbox"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

func TestHandler_ProcessBatch_eventOutbox(t *testing.T) {
	dir := t.TempDir()
	eng, err := recovery.Open(groupCommitRecoveryConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	outboxDir := t.TempDir()
	ob, err := eventoutbox.OpenFileWriter(outboxDir, eventoutbox.FileWriterConfig{SyncEveryRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()

	h := &consumer.Handler{
		Engine:      eng,
		Publisher:   publisher.NewMemory(),
		EventOutbox: ob,
		Partition:   0,
	}

	msgs := make([]kafka.Message, 2)
	for i := range msgs {
		cmd := recovery.NewOrderFromEngine(engine.Order{
			OrderID:  uint64(50 + i),
			Symbol:   "BTC-USDT",
			Side:     engine.SideSell,
			Type:     engine.OrderTypeLimit,
			Price:    recovery.MustDecimal("100"),
			Quantity: recovery.MustDecimal("1"),
		}, uint64(50+i))
		env := &matchingv1.OrderCommandEnvelope{
			Command: &matchingv1.OrderCommandEnvelope_NewOrder{NewOrder: cmd},
		}
		payload, _ := proto.Marshal(env)
		msgs[i] = kafka.Message{Partition: 0, Offset: int64(500 + i), Value: payload}
	}

	if err := h.ProcessBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	if ob.LastDurableSeq() != 2 {
		t.Fatalf("outbox durable seq=%d want 2", ob.LastDurableSeq())
	}
	recs, err := eventoutbox.FetchUnpublished(outboxDir, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("outbox records=%d want 2", len(recs))
	}
}
