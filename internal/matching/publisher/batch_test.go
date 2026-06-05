package publisher_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

type writeBatchRecorder struct {
	mu      sync.Mutex
	writes  map[string]int // topic -> WriteBatch 次数
	events  map[string]int // topic -> 事件条数
}

func newWriteBatchRecorder() *writeBatchRecorder {
	return &writeBatchRecorder{
		writes: make(map[string]int),
		events: make(map[string]int),
	}
}

func (r *writeBatchRecorder) WriteBatch(_ context.Context, topic string, _ []byte, values [][]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes[topic]++
	r.events[topic] += len(values)
	return nil
}

func (r *writeBatchRecorder) Write(context.Context, string, []byte, []byte) error { return nil }
func (r *writeBatchRecorder) Close() error                                          { return nil }

func (r *writeBatchRecorder) count(topic string) (writes, events int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writes[topic], r.events[topic]
}

func TestKafkaPublisher_PublishBatch_aggregatesPerTopic(t *testing.T) {
	rec := newWriteBatchRecorder()
	pub := &publisher.KafkaPublisher{
		Producer:   rec,
		MatchTopic: "match.events",
		TradeTopic: "trade.events",
	}

	outs := make([]publisher.Outbound, 4)
	for i := range outs {
		outs[i] = publisher.Outbound{
			MatchEvents: []*matchingv1.MatchEvent{{
				Symbol:    "BTC-USDT",
				OrderId:   uint64(i + 1),
				EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
			}},
		}
	}
	if err := pub.PublishBatch(context.Background(), outs); err != nil {
		t.Fatal(err)
	}

	matchWrites, matchEvents := rec.count("match.events")
	if matchWrites != 1 {
		t.Fatalf("match WriteBatch calls=%d want 1", matchWrites)
	}
	if matchEvents != 4 {
		t.Fatalf("match events=%d want 4", matchEvents)
	}
	if tradeWrites, _ := rec.count("trade.events"); tradeWrites != 0 {
		t.Fatalf("trade WriteBatch calls=%d want 0", tradeWrites)
	}
}

func TestKafkaPublisher_PublishBatch_preservesSymbolOrder(t *testing.T) {
	rec := newWriteBatchRecorder()
	pub := &publisher.KafkaPublisher{
		Producer:   rec,
		MatchTopic: "match.events",
		TradeTopic: "trade.events",
	}

	outs := []publisher.Outbound{
		{MatchEvents: []*matchingv1.MatchEvent{{Symbol: "BTC-USDT", OrderId: 1, EventType: matchingv1.MatchEventType_ORDER_ACCEPTED}}},
		{MatchEvents: []*matchingv1.MatchEvent{{Symbol: "ETH-USDT", OrderId: 2, EventType: matchingv1.MatchEventType_ORDER_ACCEPTED}}},
		{MatchEvents: []*matchingv1.MatchEvent{{Symbol: "BTC-USDT", OrderId: 3, EventType: matchingv1.MatchEventType_ORDER_ACCEPTED}}},
	}
	if err := pub.PublishBatch(context.Background(), outs); err != nil {
		t.Fatal(err)
	}
	matchWrites, matchEvents := rec.count("match.events")
	if matchWrites != 2 {
		t.Fatalf("match WriteBatch calls=%d want 2 (per symbol)", matchWrites)
	}
	if matchEvents != 3 {
		t.Fatalf("match events=%d want 3", matchEvents)
	}
}

func TestKafkaPublisher_PublishBatch_matchAndTradeParallel(t *testing.T) {
	prod := &stallProducer{delay: 40 * time.Millisecond}
	pub := &publisher.KafkaPublisher{
		Producer:   prod,
		MatchTopic: "match.events",
		TradeTopic: "trade.events",
	}
	outs := []publisher.Outbound{
		{
			MatchEvents: []*matchingv1.MatchEvent{{Symbol: "BTC-USDT", OrderId: 1, EventType: matchingv1.MatchEventType_ORDER_ACCEPTED}},
			TradeEvents: []*matchingv1.TradeEvent{{Trade: &commonv1.Trade{Symbol: "BTC-USDT"}}},
		},
		{
			MatchEvents: []*matchingv1.MatchEvent{{Symbol: "BTC-USDT", OrderId: 2, EventType: matchingv1.MatchEventType_ORDER_ACCEPTED}},
			TradeEvents: []*matchingv1.TradeEvent{{Trade: &commonv1.Trade{Symbol: "BTC-USDT"}}},
		},
	}
	if err := pub.PublishBatch(context.Background(), outs); err != nil {
		t.Fatal(err)
	}
	if prod.MaxInFlight() < 2 {
		t.Fatalf("expected parallel topic flush, maxInFlight=%d", prod.MaxInFlight())
	}
}
