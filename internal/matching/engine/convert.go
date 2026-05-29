package engine

import (
	"fmt"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

// OrderFromProto converts a protobuf Order to the in-memory engine Order.
func OrderFromProto(pb *commonv1.Order) (Order, error) {
	if pb == nil {
		return Order{}, fmt.Errorf("order is nil")
	}
	if pb.GetOrderId() == 0 {
		return Order{}, fmt.Errorf("order_id is required")
	}
	if pb.GetSymbol() == "" {
		return Order{}, fmt.Errorf("symbol is required")
	}

	side, err := sideFromProto(pb.GetSide())
	if err != nil {
		return Order{}, err
	}
	typ, err := orderTypeFromProto(pb.GetType())
	if err != nil {
		return Order{}, err
	}

	price, err := priceFromOrderProto(pb)
	if err != nil {
		return Order{}, err
	}
	qty, err := decimalFromProto(pb.GetQuantity(), "quantity")
	if err != nil {
		return Order{}, err
	}
	remaining, err := remainingFromProto(pb.GetRemaining(), qty)
	if err != nil {
		return Order{}, err
	}

	return Order{
		OrderID:       pb.GetOrderId(),
		ClientOrderID: pb.GetClientOrderId(),
		Symbol:        pb.GetSymbol(),
		CreateTime:    pb.GetCreateTime().AsTime(),
		UpdateTime:    pb.GetUpdateTime().AsTime(),
		Side:          side,
		Type:          typ,
		Price:         price,
		Quantity:      qty,
		Remaining:     remaining,
		Flags:         pb.GetFlags(),
	}, nil
}

// OrderToProto converts an engine Order to protobuf (e.g. for events or logging).
func OrderToProto(o Order) *commonv1.Order {
	return &commonv1.Order{
		OrderId:       o.OrderID,
		ClientOrderId: o.ClientOrderID,
		Symbol:        o.Symbol,
		CreateTime:    timestamppb.New(o.CreateTime),
		UpdateTime:    timestamppb.New(o.UpdateTime),
		Side:          sideToProto(o.Side),
		Type:          orderTypeToProto(o.Type),
		Price:         decimalToProto(o.Price),
		Quantity:      decimalToProto(o.Quantity),
		Remaining:     decimalToProto(effectiveRemaining(o)),
		Flags:         o.Flags,
	}
}

func TradeFromProto(pb *commonv1.Trade) (Trade, error) {
	if pb == nil {
		return Trade{}, fmt.Errorf("trade is nil")
	}
	if pb.GetTradeId() == 0 {
		return Trade{}, fmt.Errorf("trade_id is required")
	}
	if pb.GetSymbol() == "" {
		return Trade{}, fmt.Errorf("symbol is required")
	}

	price, err := decimalFromProto(pb.GetPrice(), "price")
	if err != nil {
		return Trade{}, err
	}
	qty, err := decimalFromProto(pb.GetQuantity(), "quantity")
	if err != nil {
		return Trade{}, err
	}

	return Trade{
		TradeID:      pb.TradeId,
		Symbol:       pb.Symbol,
		CreateTime:   pb.GetCreateTime().AsTime(),
		Price:        price,
		Quantity:     qty,
		MakerOrderID: pb.MakerOrderId,
		TakerOrderID: pb.TakerOrderId,
	}, nil
}

func TradeToProto(t Trade) *commonv1.Trade {
	return &commonv1.Trade{
		TradeId:      t.TradeID,
		Symbol:       t.Symbol,
		CreateTime:   timestamppb.New(t.CreateTime),
		Price:        decimalToProto(t.Price),
		Quantity:     decimalToProto(t.Quantity),
		MakerOrderId: t.MakerOrderID,
		TakerOrderId: t.TakerOrderID,
	}
}

func sideFromProto(s commonv1.Side) (Side, error) {
	switch s {
	case commonv1.Side_SIDE_BUY:
		return SideBuy, nil
	case commonv1.Side_SIDE_SELL:
		return SideSell, nil
	default:
		return 0, fmt.Errorf("invalid side: %v", s)
	}
}

func sideToProto(s Side) commonv1.Side {
	switch s {
	case SideBuy:
		return commonv1.Side_SIDE_BUY
	case SideSell:
		return commonv1.Side_SIDE_SELL
	default:
		return commonv1.Side_SIDE_UNSPECIFIED
	}
}

func orderTypeFromProto(t commonv1.OrderType) (OrderType, error) {
	switch t {
	case commonv1.OrderType_ORDER_TYPE_LIMIT:
		return OrderTypeLimit, nil
	case commonv1.OrderType_ORDER_TYPE_MARKET:
		return OrderTypeMarket, nil
	default:
		return 0, fmt.Errorf("invalid order type: %v", t)
	}
}

func orderTypeToProto(t OrderType) commonv1.OrderType {
	switch t {
	case OrderTypeLimit:
		return commonv1.OrderType_ORDER_TYPE_LIMIT
	case OrderTypeMarket:
		return commonv1.OrderType_ORDER_TYPE_MARKET
	default:
		return commonv1.OrderType_ORDER_TYPE_UNSPECIFIED
	}
}

func remainingFromProto(d *commonv1.Decimal, quantity decimal.Decimal) (decimal.Decimal, error) {
	if d == nil || d.GetValue() == "" {
		return quantity, nil
	}
	v, err := decimal.NewFromString(d.GetValue())
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("invalid remaining %q: %w", d.GetValue(), err)
	}
	if !v.IsPositive() {
		// proto 中 remaining 为 0 或未初始化时，视为整单未成交
		return quantity, nil
	}
	return v, nil
}

// effectiveRemaining 导出 proto 时：Remaining 未设则用 Quantity。
func effectiveRemaining(o Order) decimal.Decimal {
	if o.Remaining.IsPositive() {
		return o.Remaining
	}
	return o.Quantity
}

// priceFromOrderProto 限价单必须有 price；市价单允许缺省（撮合按对手盘价成交）。
func priceFromOrderProto(pb *commonv1.Order) (decimal.Decimal, error) {
	if pb.GetType() == commonv1.OrderType_ORDER_TYPE_MARKET {
		if d := pb.GetPrice(); d != nil && d.GetValue() != "" {
			return decimalFromProto(d, "price")
		}
		return decimal.Zero, nil
	}
	return decimalFromProto(pb.GetPrice(), "price")
}

func decimalFromProto(d *commonv1.Decimal, field string) (decimal.Decimal, error) {
	if d == nil || d.GetValue() == "" {
		return decimal.Decimal{}, fmt.Errorf("%s is required", field)
	}
	v, err := decimal.NewFromString(d.GetValue())
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("invalid %s %q: %w", field, d.GetValue(), err)
	}
	if !v.IsPositive() {
		return decimal.Decimal{}, fmt.Errorf("%s must be positive: %s", field, v.String())
	}
	return v, nil
}

func decimalToProto(v decimal.Decimal) *commonv1.Decimal {
	return &commonv1.Decimal{Value: v.String()}
}
