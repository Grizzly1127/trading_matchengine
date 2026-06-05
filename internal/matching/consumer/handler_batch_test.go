package consumer_test

import (
	"context"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

func groupCommitRecoveryConfig(dir string) recovery.Config {
	cfg := testRecoveryConfig(dir)
	cfg.WALGroupCommit = recovery.WALGroupCommitConfig{SyncEveryRecords: 8}
	return cfg
}

type batchPublishSpy struct {
	publisher.Publisher
	batchCalls int
}

func (s *batchPublishSpy) Publish(ctx context.Context, out publisher.Outbound) error {
	s.batchCalls++
	return s.Publisher.Publish(ctx, out)
}

func (s *batchPublishSpy) PublishBatch(ctx context.Context, outs []publisher.Outbound) error {
	s.batchCalls++
	return s.Publisher.PublishBatch(ctx, outs)
}

type kafkaBatchCounter struct {
	matchWrites int
	tradeWrites int
}

func (c *kafkaBatchCounter) WriteBatch(_ context.Context, topic string, _ []byte, values [][]byte) error {
	switch topic {
	case "match.events":
		c.matchWrites++
	case "trade.events":
		c.tradeWrites++
	}
	return nil
}

func (c *kafkaBatchCounter) Write(context.Context, string, []byte, []byte) error { return nil }
func (c *kafkaBatchCounter) Close() error                                          { return nil }

func TestHandler_ProcessBatch_singlePublishBatch(t *testing.T) {
	dir := t.TempDir()
	eng, err := recovery.Open(groupCommitRecoveryConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	mem := publisher.NewMemory()
	spy := &batchPublishSpy{Publisher: mem}
	h := &consumer.Handler{Engine: eng, Publisher: spy, Partition: 0}

	msgs := make([]kafka.Message, 3)
	for i := range msgs {
		cmd := recovery.NewOrderFromEngine(engine.Order{
			OrderID:  uint64(10 + i),
			Symbol:   "BTC-USDT",
			Side:     engine.SideSell,
			Type:     engine.OrderTypeLimit,
			Price:    recovery.MustDecimal("100"),
			Quantity: recovery.MustDecimal("1"),
		}, uint64(10+i))
		env := &matchingv1.OrderCommandEnvelope{
			Command: &matchingv1.OrderCommandEnvelope_NewOrder{NewOrder: cmd},
		}
		payload, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		msgs[i] = kafka.Message{Topic: "order.commands", Partition: 0, Offset: int64(200 + i), Value: payload}
	}

	if err := h.ProcessBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	if spy.batchCalls != 1 {
		t.Fatalf("PublishBatch calls=%d want 1", spy.batchCalls)
	}
	if len(mem.MatchEvents()) != 3 {
		t.Fatalf("match events=%d want 3", len(mem.MatchEvents()))
	}
}

func TestHandler_ProcessBatch_kafkaOneWritePerTopic(t *testing.T) {
	dir := t.TempDir()
	eng, err := recovery.Open(groupCommitRecoveryConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	counter := &kafkaBatchCounter{}
	kpub := &publisher.KafkaPublisher{
		Producer:   counter,
		MatchTopic: "match.events",
		TradeTopic: "trade.events",
	}
	h := &consumer.Handler{Engine: eng, Publisher: kpub, Partition: 0}

	msgs := make([]kafka.Message, 4)
	for i := range msgs {
		cmd := recovery.NewOrderFromEngine(engine.Order{
			OrderID:  uint64(20 + i),
			Symbol:   "BTC-USDT",
			Side:     engine.SideSell,
			Type:     engine.OrderTypeLimit,
			Price:    recovery.MustDecimal("100"),
			Quantity: recovery.MustDecimal("1"),
		}, uint64(20+i))
		env := &matchingv1.OrderCommandEnvelope{
			Command: &matchingv1.OrderCommandEnvelope_NewOrder{NewOrder: cmd},
		}
		payload, _ := proto.Marshal(env)
		msgs[i] = kafka.Message{Partition: 0, Offset: int64(300 + i), Value: payload}
	}

	if err := h.ProcessBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	// 4 条限价卖单无成交：仅 match topic 各 1 次 WriteBatch
	if counter.matchWrites != 1 {
		t.Fatalf("match WriteBatch=%d want 1", counter.matchWrites)
	}
	if counter.tradeWrites != 0 {
		t.Fatalf("trade WriteBatch=%d want 0", counter.tradeWrites)
	}
}
