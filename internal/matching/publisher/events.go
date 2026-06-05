package publisher

import (
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/symbol"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// Outbound 待发布的 Kafka 事件批次。
type Outbound struct {
	MatchEvents []*matchingv1.MatchEvent
	TradeEvents []*matchingv1.TradeEvent
}

// BuildNewOrderEvents 根据撮合结果构建 match/trade 事件；duplicate 为 true 时不发布。
func BuildNewOrderEvents(shard *symbol.Shard, cmd *matchingv1.NewOrderCommand, trades []engine.Trade, walSeq uint64, duplicate bool) Outbound {
	if duplicate || cmd == nil || cmd.GetOrder() == nil {
		return Outbound{}
	}

	orderID := cmd.GetOrder().GetOrderId()
	symbolName := cmd.GetOrder().GetSymbol()
	commandID := cmd.GetCommandId()
	if commandID == 0 {
		commandID = orderID
	}

	// 上界：accepted + 每笔 trade 的 maker + 可选 taker
	nMatch := 1 + len(trades)
	if len(trades) > 0 {
		nMatch++
	}

	out := Outbound{
		MatchEvents: make([]*matchingv1.MatchEvent, 0, nMatch),
		TradeEvents: make([]*matchingv1.TradeEvent, 0, len(trades)),
	}
	out.MatchEvents = append(out.MatchEvents,
		newMatchEvent(commandID, symbolName, orderID, matchingv1.MatchEventType_ORDER_ACCEPTED, cmd.GetOrder(), walSeq),
	)

	for _, tr := range trades {
		out.TradeEvents = append(out.TradeEvents, &matchingv1.TradeEvent{
			Trade:  engine.TradeToProto(tr),
			WalSeq: walSeq,
		})
	}

	seenMaker := make(map[uint64]struct{}, len(trades))
	for _, tr := range trades {
		if _, ok := seenMaker[tr.MakerOrderID]; ok {
			continue
		}
		seenMaker[tr.MakerOrderID] = struct{}{}
		out.MatchEvents = append(out.MatchEvents, makerFillEvent(shard, symbolName, tr, commandID, walSeq))
	}

	if len(trades) > 0 {
		out.MatchEvents = append(out.MatchEvents, takerFillEvent(shard, symbolName, orderID, commandID, cmd.GetOrder(), walSeq))
	}

	return out
}

// BuildCancelEvents 构建撤单事件。
func BuildCancelEvents(cmd *matchingv1.CancelOrderCommand, walSeq uint64) Outbound {
	if cmd == nil {
		return Outbound{}
	}
	commandID := cmd.GetCommandId()
	if commandID == 0 {
		commandID = cmd.GetOrderId()
	}
	return Outbound{
		MatchEvents: []*matchingv1.MatchEvent{
			newMatchEvent(commandID, cmd.GetSymbol(), cmd.GetOrderId(), matchingv1.MatchEventType_ORDER_CANCELED, nil, walSeq),
		},
	}
}

// takerFillEvent 构建成交事件。
func takerFillEvent(shard *symbol.Shard, symbolName string, orderID, commandID uint64, order *commonv1.Order, walSeq uint64) *matchingv1.MatchEvent {
	typ := matchingv1.MatchEventType_ORDER_FILLED
	if orderActive(shard, symbolName, orderID) {
		typ = matchingv1.MatchEventType_ORDER_PARTIAL_FILLED
	}
	return newMatchEvent(commandID, symbolName, orderID, typ, order, walSeq)
}

// makerFillEvent 构建 maker 成交事件。
func makerFillEvent(shard *symbol.Shard, symbolName string, tr engine.Trade, commandID, walSeq uint64) *matchingv1.MatchEvent {
	orderID := tr.MakerOrderID
	typ := matchingv1.MatchEventType_ORDER_FILLED
	if orderActive(shard, symbolName, orderID) {
		typ = matchingv1.MatchEventType_ORDER_PARTIAL_FILLED
	}
	var order *commonv1.Order
	if se, ok := shard.Get(symbolName); ok {
		o, ok := se.OrderBook.FindOrder(orderID)
		if ok {
			order = engine.OrderToProto(o)
		}
	}
	// Maker 已全成并从盘口移除时，用本笔成交量构造 remaining=0，供 Order Service 回写 filled_quantity。
	if order == nil && typ == matchingv1.MatchEventType_ORDER_FILLED {
		order = &commonv1.Order{
			OrderId:   orderID,
			Symbol:    symbolName,
			Quantity:  &commonv1.Decimal{Value: tr.Quantity.String()},
			Remaining: &commonv1.Decimal{Value: "0"},
		}
	}
	return newMatchEvent(commandID, symbolName, orderID, typ, order, walSeq)
}

func newMatchEvent(commandID uint64, symbolName string, orderID uint64, typ matchingv1.MatchEventType, order *commonv1.Order, walSeq uint64) *matchingv1.MatchEvent {
	return &matchingv1.MatchEvent{
		CommandId: commandID,
		Symbol:    symbolName,
		OrderId:   orderID,
		EventType: typ,
		Order:     order,
		WalSeq:    walSeq,
	}
}

func orderActive(shard *symbol.Shard, symbolName string, orderID uint64) bool {
	se, ok := shard.Get(symbolName)
	if !ok {
		return false
	}
	if _, ok := se.OrderBook.FindOrder(orderID); ok {
		return true
	}
	return false
}
