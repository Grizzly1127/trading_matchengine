// Package benchutil 提供压测用的 Kafka 命令构造与 Prometheus 指标解析。
package benchutil

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// NewOrderParams 构造下单命令所需字段。
type NewOrderParams struct {
	CommandID     uint64
	OrderID       uint64
	ClientOrderID string
	Symbol        string
	Side          commonv1.Side
	Price         string // 空串表示市价单不写 price
	Quantity      string
	Partition     uint32
	Offset        uint64
}

// MarshalNewOrderEnvelope 序列化 OrderCommandEnvelope（new_order）。
func MarshalNewOrderEnvelope(p NewOrderParams) ([]byte, error) {
	now := time.Now().UTC()
	pbOrder := &commonv1.Order{
		OrderId:       p.OrderID,
		ClientOrderId: p.ClientOrderID,
		Symbol:        p.Symbol,
		CreateTime:    timestamppb.New(now),
		UpdateTime:    timestamppb.New(now),
		Side:          p.Side,
		Type:          commonv1.OrderType_ORDER_TYPE_LIMIT,
		Quantity:      &commonv1.Decimal{Value: p.Quantity},
		Remaining:     &commonv1.Decimal{Value: p.Quantity},
	}
	if p.Price != "" {
		pbOrder.Price = &commonv1.Decimal{Value: p.Price}
	} else {
		pbOrder.Type = commonv1.OrderType_ORDER_TYPE_MARKET
	}

	env := &matchingv1.OrderCommandEnvelope{
		Command: &matchingv1.OrderCommandEnvelope_NewOrder{
			NewOrder: &matchingv1.NewOrderCommand{
				CommandId:      p.CommandID,
				Order:          pbOrder,
				KafkaPartition: p.Partition,
				KafkaOffset:    p.Offset,
			},
		},
	}
	return proto.Marshal(env)
}

// MarshalCancelEnvelope 序列化撤单命令。
func MarshalCancelEnvelope(commandID uint64, symbol string, orderID uint64, partition uint32, offset uint64) ([]byte, error) {
	env := &matchingv1.OrderCommandEnvelope{
		Command: &matchingv1.OrderCommandEnvelope_CancelOrder{
			CancelOrder: &matchingv1.CancelOrderCommand{
				CommandId:      commandID,
				Symbol:         symbol,
				OrderId:        orderID,
				KafkaPartition: partition,
				KafkaOffset:    offset,
			},
		},
	}
	return proto.Marshal(env)
}

// ClientOrderID 生成压测用幂等 ID。
func ClientOrderID(prefix string, seq uint64) string {
	return fmt.Sprintf("%s-%d", prefix, seq)
}
