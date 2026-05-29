package recovery

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/worker"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
)

// Restore 启动时从 Redis 恢复 open bar，并同步排空待处理闭合队列。
func Restore(ctx context.Context, log zerolog.Logger, agg *aggregator.Aggregator, pub *publisher.RedisPublisher, w *worker.Worker) error {
	if agg == nil || pub == nil {
		return fmt.Errorf("recovery: aggregator and publisher are required")
	}
	var restored int
	if err := pub.ScanOpenBars(ctx, func(symbol string, iv interval.Interval, b bar.OHLCV) error {
		agg.RestoreOpen(symbol, iv, b)
		restored++
		return nil
	}); err != nil {
		return fmt.Errorf("scan open bars: %w", err)
	}
	log.Info().Int("open_bars", restored).Msg("kline open bars restored from redis")

	if w != nil {
		if err := w.DrainPending(ctx); err != nil {
			return fmt.Errorf("drain pending closed: %w", err)
		}
	}
	return nil
}
