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

type stallProducer struct {
	delay    time.Duration
	mu       sync.Mutex
	inFlight int
	maxIn    int
}

func (p *stallProducer) WriteBatch(_ context.Context, _ string, _ []byte, _ [][]byte) error {
	p.mu.Lock()
	p.inFlight++
	if p.inFlight > p.maxIn {
		p.maxIn = p.inFlight
	}
	p.mu.Unlock()

	time.Sleep(p.delay)

	p.mu.Lock()
	p.inFlight--
	p.mu.Unlock()
	return nil
}

func (p *stallProducer) Write(context.Context, string, []byte, []byte) error { return nil }
func (p *stallProducer) Close() error                                          { return nil }

func (p *stallProducer) MaxInFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxIn
}

func TestKafkaPublisher_parallelMatchAndTrade(t *testing.T) {
	prod := &stallProducer{delay: 40 * time.Millisecond}
	pub := &publisher.KafkaPublisher{
		Producer:   prod,
		MatchTopic: "match.events",
		TradeTopic: "trade.events",
	}

	out := publisher.Outbound{
		MatchEvents: []*matchingv1.MatchEvent{{
			Symbol: "BTC-USDT", OrderId: 1,
			EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
			Order:     &commonv1.Order{OrderId: 1, Symbol: "BTC-USDT"},
		}},
		TradeEvents: []*matchingv1.TradeEvent{{
			Trade: &commonv1.Trade{Symbol: "BTC-USDT"},
		}},
	}

	start := time.Now()
	if err := pub.Publish(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if prod.MaxInFlight() < 2 {
		t.Fatalf("expected concurrent WriteBatch, maxInFlight=%d", prod.MaxInFlight())
	}
	// 串行约 80ms+，并行应明显低于 70ms
	if elapsed > 70*time.Millisecond {
		t.Fatalf("parallel publish took %v, want clearly less than 2x %v", elapsed, prod.delay)
	}
}

func TestKafkaPublisher_matchOnlySerial(t *testing.T) {
	prod := &stallProducer{delay: 10 * time.Millisecond}
	pub := &publisher.KafkaPublisher{
		Producer: prod, MatchTopic: "match.events", TradeTopic: "trade.events",
	}
	out := publisher.Outbound{
		MatchEvents: []*matchingv1.MatchEvent{{
			Symbol: "BTC-USDT", OrderId: 1,
			EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
		}},
	}
	if err := pub.Publish(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	if prod.MaxInFlight() != 1 {
		t.Fatalf("maxInFlight=%d want 1", prod.MaxInFlight())
	}
}
