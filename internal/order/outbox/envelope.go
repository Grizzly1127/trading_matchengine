package outbox

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

const (
	EventTypeNewOrder    = "NewOrderCommand"
	EventTypeCancelOrder = "CancelOrderCommand"
)

// OrderSnapshot 构建 Outbox 载荷所需的订单字段快照。
type OrderSnapshot struct {
	ID            uint64
	ClientOrderID string
	Symbol        string
	Side          int16
	OrderType     int16
	Price         *string
	Quantity      string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// BuildNewOrderPayload 构建 NewOrderCommand 的 protobuf 载荷；command_id 使用 outbox.id。
func BuildNewOrderPayload(order OrderSnapshot, commandID uint64) ([]byte, error) {
	env := buildNewOrderEnvelope(order, commandID)
	return proto.Marshal(env)
}

func buildNewOrderEnvelope(order OrderSnapshot, commandID uint64) *matchingv1.OrderCommandEnvelope {
	pbOrder := &commonv1.Order{
		OrderId:       order.ID,
		ClientOrderId: order.ClientOrderID,
		Symbol:        order.Symbol,
		CreateTime:    timestamppb.New(order.CreatedAt),
		UpdateTime:    timestamppb.New(order.UpdatedAt),
		Side:          commonv1.Side(order.Side),
		Type:          commonv1.OrderType(order.OrderType),
		Quantity:      &commonv1.Decimal{Value: order.Quantity},
		Remaining:     &commonv1.Decimal{Value: order.Quantity},
	}
	if order.Price != nil {
		pbOrder.Price = &commonv1.Decimal{Value: *order.Price}
	}

	return &matchingv1.OrderCommandEnvelope{
		Command: &matchingv1.OrderCommandEnvelope_NewOrder{
			NewOrder: &matchingv1.NewOrderCommand{
				CommandId: commandID,
				Order:     pbOrder,
			},
		},
	}
}
