package consumer

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// MatchStore 应用 match.events 的持久化接口。
type MatchStore interface {
	ApplyMatchEvent(ctx context.Context, in repository.MatchEventApply) error
}

// MatchHandler 消费 match.events 并更新 orders.status。
type MatchHandler struct {
	Repo MatchStore
}

// Process 解码并应用 MatchEvent。
func (h *MatchHandler) Process(ctx context.Context, msg kafka.Message) error {
	if h == nil || h.Repo == nil {
		return fmt.Errorf("match handler: not configured")
	}
	var ev matchingv1.MatchEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("match handler: decode: %w", err)
	}
	if ev.GetOrderId() == 0 {
		return fmt.Errorf("match handler: order_id is required")
	}
	if ev.GetEventType() == matchingv1.MatchEventType_MATCH_EVENT_TYPE_UNSPECIFIED {
		return fmt.Errorf("match handler: event_type is required")
	}

	var filled *string
	if o := ev.GetOrder(); o != nil && o.GetQuantity() != nil && o.GetRemaining() != nil {
		s, err := repository.FilledQuantityFromRemaining(o.GetQuantity().GetValue(), o.GetRemaining().GetValue())
		if err != nil {
			return fmt.Errorf("match handler: filled quantity: %w", err)
		}
		filled = &s
	}

	return h.Repo.ApplyMatchEvent(ctx, repository.MatchEventApply{
		OrderID:        ev.GetOrderId(),
		Symbol:         ev.GetSymbol(),
		EventType:      int16(ev.GetEventType()),
		WalSeq:         ev.GetWalSeq(),
		FilledQuantity: filled,
	})
}
