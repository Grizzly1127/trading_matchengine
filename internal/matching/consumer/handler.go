package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/metrics"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/proto"
)

// Handler 处理 order.commands 消息：WAL 持久化后发布事件。
type Handler struct {
	Engine    *recovery.Engine
	Publisher publisher.Publisher
	Partition uint32
	Metrics   *metrics.Metrics
}

// Process 解码、撮合、发布；调用方在成功后 Commit offset。
func (h *Handler) Process(ctx context.Context, msg kafka.Message) error {
	start := time.Now()
	err := h.process(ctx, msg)
	if h.Metrics != nil {
		if err != nil {
			h.Metrics.ObserveCommandFailed()
		} else {
			h.Metrics.ObserveProcessing(time.Since(start))
			h.Metrics.SetLastProcessedOffset(uint64(msg.Offset))
		}
	}
	return err
}

func (h *Handler) process(ctx context.Context, msg kafka.Message) error {
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
	if errors.Is(err, engine.ErrSymbolReadOnly) {
		// 对账只读：不落 WAL、不发布，提交 offset 避免阻塞同 partition 其他 symbol。
		return nil
	}
	if err != nil {
		return err
	}
	walSeq := h.Engine.LastSeq()
	duplicate := walSeq == before

	if h.Metrics != nil {
		h.Metrics.ObserveTrades(len(trades))
	}
	out := publisher.BuildNewOrderEvents(h.Engine.Shard(), cmd, trades, walSeq, duplicate)
	if err := h.Publisher.Publish(ctx, out); err != nil {
		if h.Metrics != nil {
			h.Metrics.ObservePublishError()
		}
		return err
	}
	return nil
}

func (h *Handler) processCancel(ctx context.Context, cmd *matchingv1.CancelOrderCommand, msg kafka.Message) error {
	cmd.KafkaPartition = uint32(msg.Partition)
	cmd.KafkaOffset = uint64(msg.Offset)

	if err := h.Engine.ApplyCancel(cmd); err != nil {
		return err
	}
	out := publisher.BuildCancelEvents(cmd, h.Engine.LastSeq())
	if err := h.Publisher.Publish(ctx, out); err != nil {
		if h.Metrics != nil {
			h.Metrics.ObservePublishError()
		}
		return err
	}
	return nil
}
