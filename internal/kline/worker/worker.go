package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/publisher"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
)

const (
	defaultQueueSize = 4096
	maxRetries       = 3
)

// Worker 异步处理闭合 bar：落库、删 open 快照、推送。
type Worker struct {
	Repo   *repository.Repository
	Pub    *publisher.RedisPublisher
	Log    zerolog.Logger
	ch     chan aggregator.ClosedEvent
	closed chan struct{}
}

// New 创建 Worker。
func New(repo *repository.Repository, pub *publisher.RedisPublisher, log zerolog.Logger, queueSize int) *Worker {
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	return &Worker{
		Repo:   repo,
		Pub:    pub,
		Log:    log,
		ch:     make(chan aggregator.ClosedEvent, queueSize),
		closed: make(chan struct{}),
	}
}

// HandleClose 由 Aggregator 回调：先写 Redis 待处理队列，再送入内存队列。
func (w *Worker) HandleClose(ctx context.Context, ev aggregator.ClosedEvent) {
	if w == nil || w.Pub == nil {
		return
	}
	if err := w.Pub.EnqueueClosed(ctx, ev.Symbol, ev.Interval, ev.Bar); err != nil {
		w.Log.Error().Err(err).
			Str("symbol", ev.Symbol).
			Str("interval", string(ev.Interval)).
			Msg("enqueue closed bar to redis failed")
		return
	}
	select {
	case w.ch <- ev:
	default:
		// 内存队列满时依赖 Redis pending 重放，不阻塞热路径。
		w.Log.Warn().
			Str("symbol", ev.Symbol).
			Str("interval", string(ev.Interval)).
			Msg("closed bar worker queue full, will drain from redis pending")
	}
}

// Run 启动后台处理循环。
func (w *Worker) Run(ctx context.Context) {
	defer close(w.closed)
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-w.ch:
			w.processWithRetry(ctx, ev)
		default:
			j, ok, err := w.Pub.PopPendingClosed(ctx)
			if err != nil {
				w.Log.Error().Err(err).Msg("pop pending closed bar failed")
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}
			if !ok {
				select {
				case <-ctx.Done():
					return
				case ev := <-w.ch:
					w.processWithRetry(ctx, ev)
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}
			ev, err := closedFromJSON(j)
			if err != nil {
				w.Log.Error().Err(err).Msg("decode pending closed bar")
				continue
			}
			w.processWithRetry(ctx, ev)
		}
	}
}

// DrainPending 启动时同步处理 Redis 中未完成的闭合任务。
func (w *Worker) DrainPending(ctx context.Context) error {
	for {
		n, err := w.Pub.PendingCloseCount(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		j, ok, err := w.Pub.PopPendingClosed(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		ev, err := closedFromJSON(j)
		if err != nil {
			w.Log.Error().Err(err).Msg("decode pending closed bar on startup")
			continue
		}
		w.processWithRetry(ctx, ev)
	}
}

// Drain 优雅退出前排空内存与 Redis 待处理队列。
func (w *Worker) Drain(ctx context.Context) {
	for {
		n, err := w.Pub.PendingCloseCount(ctx)
		if err != nil {
			w.Log.Error().Err(err).Msg("pending close count")
			return
		}
		if n == 0 && len(w.ch) == 0 {
			return
		}
		j, ok, err := w.Pub.PopPendingClosed(ctx)
		if err != nil {
			w.Log.Error().Err(err).Msg("drain pending closed")
			return
		}
		if !ok {
			select {
			case ev := <-w.ch:
				w.processWithRetry(ctx, ev)
			default:
				return
			}
			continue
		}
		ev, err := closedFromJSON(j)
		if err != nil {
			continue
		}
		w.processWithRetry(ctx, ev)
	}
}

// WaitStopped 等待 Run 退出。
func (w *Worker) WaitStopped() {
	<-w.closed
}

func (w *Worker) processWithRetry(ctx context.Context, ev aggregator.ClosedEvent) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := w.processOne(ctx, ev); err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			continue
		}
		return
	}
	w.Log.Error().Err(lastErr).
		Str("symbol", ev.Symbol).
		Str("interval", string(ev.Interval)).
		Int64("open_time_ms", ev.Bar.OpenTimeMs).
		Msg("persist closed bar failed after retries")
}

func (w *Worker) processOne(ctx context.Context, ev aggregator.ClosedEvent) error {
	if w.Repo == nil {
		return fmt.Errorf("repository not configured")
	}
	rec := repository.ClosedBarFromOHLCV(ev.Symbol, ev.Interval, ev.Bar)
	if err := w.Repo.InsertClosed(ctx, rec); err != nil {
		return err
	}
	if w.Pub != nil {
		if err := w.Pub.PublishClosed(ctx, ev.Symbol, ev.Interval, ev.Bar); err != nil {
			return err
		}
	}
	return nil
}

func closedFromJSON(j bar.JSON) (aggregator.ClosedEvent, error) {
	iv, err := interval.Parse(j.Interval)
	if err != nil {
		return aggregator.ClosedEvent{}, err
	}
	b, err := barJSONToOHLCV(j)
	if err != nil {
		return aggregator.ClosedEvent{}, err
	}
	return aggregator.ClosedEvent{
		Symbol:   j.Symbol,
		Interval: iv,
		Bar:      b,
	}, nil
}

func barJSONToOHLCV(j bar.JSON) (bar.OHLCV, error) {
	open, err := bar.ParseDecimal(j.Open)
	if err != nil {
		return bar.OHLCV{}, err
	}
	high, err := bar.ParseDecimal(j.High)
	if err != nil {
		return bar.OHLCV{}, err
	}
	low, err := bar.ParseDecimal(j.Low)
	if err != nil {
		return bar.OHLCV{}, err
	}
	closeP, err := bar.ParseDecimal(j.Close)
	if err != nil {
		return bar.OHLCV{}, err
	}
	vol, err := bar.ParseDecimal(j.Volume)
	if err != nil {
		return bar.OHLCV{}, err
	}
	return bar.OHLCV{
		OpenTimeMs:  j.OpenTimeMs,
		Open:        open,
		High:        high,
		Low:         low,
		Close:       closeP,
		Volume:      vol,
		UpdatedAtMs: j.TimestampMs,
	}, nil
}
