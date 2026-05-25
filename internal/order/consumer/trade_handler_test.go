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

type fakeTradeStore struct {
	applied []repository.TradeEventApply
}

func (f *fakeTradeStore) ApplyTradeEvent(_ context.Context, in repository.TradeEventApply) error {
	f.applied = append(f.applied, in)
	return nil
}

func TestTradeHandler_Process(t *testing.T) {
	ev := &matchingv1.TradeEvent{
		Trade: &commonv1.Trade{
			TradeId:       99,
			Symbol:        "BTC-USDT",
			MakerOrderId:  1,
			TakerOrderId:  2,
			Price:         &commonv1.Decimal{Value: "100"},
			Quantity:      &commonv1.Decimal{Value: "0.1"},
		},
		WalSeq: 7,
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	store := &fakeTradeStore{}
	h := &TradeHandler{Repo: store}
	if err := h.Process(context.Background(), kafka.Message{Value: b}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(store.applied) != 1 || store.applied[0].TradeID != 99 {
		t.Fatalf("applied=%+v", store.applied)
	}
}

func TestTradeHandler_MissingTradeID(t *testing.T) {
	ev := &matchingv1.TradeEvent{Trade: &commonv1.Trade{Symbol: "BTC-USDT"}}
	b, _ := proto.Marshal(ev)
	h := &TradeHandler{Repo: &fakeTradeStore{}}
	if err := h.Process(context.Background(), kafka.Message{Value: b}); err == nil {
		t.Fatal("expected error when trade_id missing")
	}
}
