package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// Processor 处理单条 Kafka 消息。
type Processor interface {
	Process(ctx context.Context, msg kafka.Message) error
}

// StaleEventError 表示可跳过的陈旧事件（如 reset 后 DB 已空但 Kafka 仍有历史消息）。
func StaleEventError(err error) bool {
	return errors.Is(err, repository.ErrOrderNotFound) ||
		errors.Is(err, repository.ErrSkippableEvent)
}

// Run 循环消费：处理成功后才 Commit offset。
func Run(ctx context.Context, c kafka.Consumer, h Processor) error {
	if c == nil || h == nil {
		return fmt.Errorf("consumer: consumer and handler are required")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := c.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: read: %w", err)
		}
		if err := h.Process(ctx, msg); err != nil {
			if StaleEventError(err) {
				if err := c.Commit(ctx, msg); err != nil {
					return fmt.Errorf("consumer: commit skipped stale offset %d: %w", msg.Offset, err)
				}
				continue
			}
			return fmt.Errorf("consumer: process offset %d: %w", msg.Offset, err)
		}
		if err := c.Commit(ctx, msg); err != nil {
			return fmt.Errorf("consumer: commit offset %d: %w", msg.Offset, err)
		}
	}
}
