package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

type Processor interface {
	Process(ctx context.Context, msg kafka.Message) error
}

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
			if errors.Is(err, ErrSkipMatchEvent) {
				if err := c.Commit(ctx, msg); err != nil {
					return fmt.Errorf("consumer: commit skipped offset %d: %w", msg.Offset, err)
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
