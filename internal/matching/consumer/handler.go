package consumer

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	"google.golang.org/protobuf/proto"
)

// Handler 处理 order.commands 消息：WAL 持久化后发布事件。
type Handler struct {
	Engine    *recovery.Engine
	Publisher publisher.Publisher
	Partition uint32
}

// Process 解码、撮合、发布；调用方在成功后 Commit offset。
func (h *Handler) Process(ctx context.Context, msg kafka.Message) error {
	var env matchingv1.OrderCommandEnvelope
	if err := proto.Unmarshal(msg.Value, &env); err != nil {
		return fmt.Errorf("consumer: decode envelope: %w", err)
	}

	if no := env.GetNewOrder(); no != nil {
		return h.processNewOrder(ctx, no, msg)
	}
	if co := env.GetCancelOrder(); co != nil {
		return h.processCancel(ctx, co, msg)
	}
	return fmt.Errorf("consumer: empty envelope at offset %d", msg.Offset)
}

func (h *Handler) processNewOrder(ctx context.Context, cmd *matchingv1.NewOrderCommand, msg kafka.Message) error {
	cmd.KafkaPartition = uint32(msg.Partition)
	cmd.KafkaOffset = uint64(msg.Offset)

	before := h.Engine.LastSeq()
	trades, err := h.Engine.ApplyNewOrder(cmd)
	if err != nil {
		return err
	}
	walSeq := h.Engine.LastSeq()
	duplicate := walSeq == before

	out := publisher.BuildNewOrderEvents(h.Engine.Shard(), cmd, trades, walSeq, duplicate)
	return h.Publisher.Publish(ctx, out)
}

func (h *Handler) processCancel(ctx context.Context, cmd *matchingv1.CancelOrderCommand, msg kafka.Message) error {
	cmd.KafkaPartition = uint32(msg.Partition)
	cmd.KafkaOffset = uint64(msg.Offset)

	if err := h.Engine.ApplyCancel(cmd); err != nil {
		return err
	}
	out := publisher.BuildCancelEvents(cmd, h.Engine.LastSeq())
	return h.Publisher.Publish(ctx, out)
}
