package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// Run 循环消费：处理成功后才 Commit offset。
func Run(ctx context.Context, c kafka.Consumer, h *Handler) error {
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
			return fmt.Errorf("consumer: process offset %d: %w", msg.Offset, err)
		}
		if err := c.Commit(ctx, msg); err != nil {
			return fmt.Errorf("consumer: commit offset %d: %w", msg.Offset, err)
		}
	}
}

// StartOffset 根据 WAL 中已记录的 kafka offset 计算下一条消费位点。
func StartOffset(resume uint64, hasResume bool) int64 {
	if !hasResume {
		// 空 WAL（含 reset-dev 后）：从最早消费，避免 topic 残留命令在「最新」策略下被跳过。
		return 0
	}
	return int64(resume + 1)
}

// ErrClosed 表示 consumer 已关闭。
var ErrClosed = errors.New("consumer: closed")
