package consumer

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
)

type fakeMatchStore struct {
	applied []repository.MatchEventApply
}

func (f *fakeMatchStore) ApplyMatchEvent(_ context.Context, in repository.MatchEventApply) error {
	f.applied = append(f.applied, in)
	return nil
}

func TestMatchHandler_Process(t *testing.T) {
	ev := &matchingv1.MatchEvent{
		OrderId:   1,
		Symbol:    "BTC-USDT",
		EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
		WalSeq:    10,
		Order: &commonv1.Order{
			OrderId:   1,
			Symbol:    "BTC-USDT",
			Quantity:  &commonv1.Decimal{Value: "1"},
			Remaining: &commonv1.Decimal{Value: "1"},
		},
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	store := &fakeMatchStore{}
	h := &MatchHandler{Repo: store}
	if err := h.Process(context.Background(), kafka.Message{Value: b}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(store.applied) != 1 {
		t.Fatalf("applied=%d want 1", len(store.applied))
	}
	if store.applied[0].EventType != int16(matchingv1.MatchEventType_ORDER_ACCEPTED) {
		t.Fatalf("event_type=%d", store.applied[0].EventType)
	}
}

func TestMatchHandler_InvalidPayload(t *testing.T) {
	h := &MatchHandler{Repo: &fakeMatchStore{}}
	if err := h.Process(context.Background(), kafka.Message{Value: []byte("bad")}); err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

func TestMatchHandler_NotConfigured(t *testing.T) {
	h := &MatchHandler{}
	if err := h.Process(context.Background(), kafka.Message{Value: []byte{}}); err == nil {
		t.Fatal("expected error")
	}
}
