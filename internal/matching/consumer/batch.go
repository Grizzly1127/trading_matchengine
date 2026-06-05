package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// StartOffset 根据 WAL 中已记录的 kafka offset 计算下一条消费位点。
func StartOffset(resume uint64, hasResume bool) int64 {
	if !hasResume {
		// 空 WAL（含 reset-dev 后）：从最早消费，避免 topic 残留命令在「最新」策略下被跳过。
		return 0
	}
	return int64(resume + 1)
}

// RunOptions 消费循环配置。
type RunOptions struct {
	// BatchMax 组提交模式下每批最多消息数；<=1 表示逐条处理。
	BatchMax int
	// BatchWait 组提交模式下凑批最长等待。
	BatchWait time.Duration
}

// Run 消费 order.commands；组提交启用时使用批量 ProcessBatch。
func Run(ctx context.Context, c kafka.Consumer, h *Handler, opts RunOptions) error {
	if c == nil || h == nil {
		return fmt.Errorf("consumer: consumer and handler are required")
	}
	if h.Engine != nil && h.Engine.GroupCommitEnabled() && opts.BatchMax > 1 {
		return runBatched(ctx, c, h, opts)
	}
	return runSequential(ctx, c, h)
}

func runSequential(ctx context.Context, c kafka.Consumer, h *Handler) error {
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

func runBatched(ctx context.Context, c kafka.Consumer, h *Handler, opts RunOptions) error {
	batch := make([]kafka.Message, 0, opts.BatchMax)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := h.ProcessBatch(ctx, batch); err != nil {
			return err
		}
		for _, msg := range batch {
			if err := c.Commit(ctx, msg); err != nil {
				return fmt.Errorf("consumer: commit offset %d: %w", msg.Offset, err)
			}
		}
		batch = batch[:0]
		return nil
	}

	pollWait := opts.BatchWait
	if pollWait <= 0 {
		pollWait = 2 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return flush()
		default:
		}

		readCtx, cancel := context.WithTimeout(ctx, pollWait)
		msg, err := c.Read(readCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return flush()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if err := flush(); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("consumer: read: %w", err)
		}

		batch = append(batch, msg)
		if len(batch) >= opts.BatchMax {
			if err := flush(); err != nil {
				return fmt.Errorf("consumer: batch: %w", err)
			}
		}
	}
}
