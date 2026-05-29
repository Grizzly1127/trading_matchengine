package consumer

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// MatchHandler 消费 match.events 并维护内存 orderbook 镜像。
type MatchHandler struct {
	Store   *store.Store
	Metrics *metrics.Counters
}

func (h *MatchHandler) Process(ctx context.Context, msg kafka.Message) error {
	_ = ctx
	if h == nil || h.Store == nil {
		return fmt.Errorf("match handler: not configured")
	}
	var ev matchingv1.MatchEvent
	if err := proto.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("match handler: decode: %w", err)
	}
	if ev.GetOrderId() == 0 {
		return fmt.Errorf("match handler: order_id is required")
	}
	if ev.GetSymbol() == "" {
		return fmt.Errorf("match handler: symbol is required")
	}

	switch ev.GetEventType() {
	case matchingv1.MatchEventType_ORDER_ACCEPTED:
		err := h.applyAccepted(&ev)
		h.incMatchEvent(err)
		return err
	case matchingv1.MatchEventType_ORDER_PARTIAL_FILLED:
		err := h.applyPartial(&ev)
		h.incMatchEvent(err)
		return err
	case matchingv1.MatchEventType_ORDER_FILLED, matchingv1.MatchEventType_ORDER_CANCELED:
		err := h.Store.ApplyOrderBookRemove(ev.GetSymbol(), ev.GetOrderId())
		h.incMatchEvent(err)
		return err
	default:
		// 未知事件：忽略（便于前向兼容）。
		return nil
	}
}

func (h *MatchHandler) applyAccepted(ev *matchingv1.MatchEvent) error {
	o := ev.GetOrder()
	// ACCEPTED 必须带 order；若缺失则忽略。
	if o == nil {
		return nil
	}
	if o.GetPrice() == nil || o.GetRemaining() == nil {
		return fmt.Errorf("match handler: accepted order missing price/remaining")
	}
	sideStr := sideToString(o.GetSide())
	return h.Store.ApplyOrderBookAccepted(
		ev.GetSymbol(),
		ev.GetOrderId(),
		sideStr,
		o.GetPrice().GetValue(),
		o.GetRemaining().GetValue(),
	)
}

func (h *MatchHandler) applyPartial(ev *matchingv1.MatchEvent) error {
	// PARTIAL 可能带 order（taker），也可能 nil（maker）；若 nil 则仅依赖内存已存在 entry。
	o := ev.GetOrder()
	if o == nil || o.GetRemaining() == nil {
		// 尝试按 order_id 更新 remaining 无法得知新值，因此忽略。
		// 后续如果需要更严格一致性，可在 match.events 增补 remaining 字段。
		return nil
	}
	return h.Store.ApplyOrderBookRemaining(ev.GetSymbol(), ev.GetOrderId(), o.GetRemaining().GetValue())
}

func sideToString(s commonv1.Side) string {
	switch s {
	case commonv1.Side_SIDE_BUY:
		return "BUY"
	case commonv1.Side_SIDE_SELL:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}

func (h *MatchHandler) incMatchEvent(err error) {
	if err == nil && h.Metrics != nil {
		h.Metrics.MatchEvents.Add(1)
	}
}
