package consumer

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

func TestMatchHandler_MarketAcceptedSkipped(t *testing.T) {
	ev := &matchingv1.MatchEvent{
		OrderId:   9,
		Symbol:    "BTC-USDT",
		EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
		Order: &commonv1.Order{
			OrderId:  9,
			Symbol:   "BTC-USDT",
			Side:     commonv1.Side_SIDE_BUY,
			Type:     commonv1.OrderType_ORDER_TYPE_MARKET,
			Quantity: &commonv1.Decimal{Value: "0.01"},
		},
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	h := &MatchHandler{Store: store.New(), Metrics: &metrics.Counters{}}
	if err := h.Process(context.Background(), kafka.Message{Value: b}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if _, ok := h.Store.SnapshotOrderBook("BTC-USDT", 10); ok && len(h.Store.Symbols()) > 0 {
		snap, _ := h.Store.SnapshotOrderBook("BTC-USDT", 10)
		if len(snap.Asks)+len(snap.Bids) > 0 {
			t.Fatal("market order should not be on book")
		}
	}
}

func TestMatchHandler_LimitAcceptedMissingPriceSkippable(t *testing.T) {
	ev := &matchingv1.MatchEvent{
		OrderId:   10,
		Symbol:    "BTC-USDT",
		EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
		Order: &commonv1.Order{
			OrderId:  10,
			Symbol:   "BTC-USDT",
			Side:     commonv1.Side_SIDE_BUY,
			Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
			Quantity: &commonv1.Decimal{Value: "0.01"},
		},
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	h := &MatchHandler{Store: store.New()}
	err = h.Process(context.Background(), kafka.Message{Value: b})
	if !errors.Is(err, ErrSkipMatchEvent) {
		t.Fatalf("err=%v want ErrSkipMatchEvent", err)
	}
}
