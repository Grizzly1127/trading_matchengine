package publisher

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// CommandPublisher 将订单命令直接发布到 order.commands（第 4 步 4.1，无 Outbox）。
type CommandPublisher struct {
	Writer    *kafka.EventWriter
	Topic     string
	Partition int
}

// PublishNewOrder 发布 NewOrderCommand（OrderCommandEnvelope 包装）。
func (p *CommandPublisher) PublishNewOrder(ctx context.Context, order *repository.Order) error {
	if p == nil || p.Writer == nil {
		return fmt.Errorf("command publisher: writer is nil")
	}
	if order == nil {
		return fmt.Errorf("command publisher: order is nil")
	}

	env := buildNewOrderEnvelope(order)
	payload, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	if err := p.Writer.Write(ctx, p.Topic, []byte(order.Symbol), payload); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	return nil
}

func buildNewOrderEnvelope(order *repository.Order) *matchingv1.OrderCommandEnvelope {
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
				CommandId: order.ID,
				Order:     pbOrder,
			},
		},
	}
}
