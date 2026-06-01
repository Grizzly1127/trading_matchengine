package reconciler

import (
	"context"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/rs/zerolog"
)

// Store 超时补偿所需持久化操作。
type Store interface {
	FindStalePendingForReject(ctx context.Context, olderThan time.Time, limit int) ([]repository.StalePendingOrder, error)
	RejectStalePending(ctx context.Context, orderID uint64, outboxTopic string) (bool, error)
	FindStuckCancelingForResend(ctx context.Context, olderThan time.Time, limit int) ([]uint64, error)
	ResendCancelCommand(ctx context.Context, orderID uint64, outboxTopic string) (bool, error)
	CountStaleUnpublishedOutbox(ctx context.Context, olderThan time.Time) (int, error)
}

// Config 超时补偿调度参数（对齐 architecture-spec §4.5）。
type Config struct {
	Enabled                     bool
	Interval                    time.Duration
	BatchSize                   int
	PendingAcceptTimeout        time.Duration
	CancelConfirmTimeout        time.Duration
	OutboxStaleWarn             time.Duration
	CommandTopic                string
}

// Scheduler 周期性扫描中间态订单并补偿。
type Scheduler struct {
	Store    Store
	Matching MatchingAdmin
	Log      zerolog.Logger
	Config   Config
}

// Run 阻塞运行直至 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	if s == nil || s.Store == nil || !s.Config.Enabled {
		return
	}
	cfg := s.normalizedConfig()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	s.tick(ctx, cfg)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx, cfg)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context, cfg Config) {
	now := time.Now()
	s.warnStaleOutbox(ctx, cfg, now)
	s.rejectStalePending(ctx, cfg, now)
	s.resendStuckCancel(ctx, cfg, now)
}

func (s *Scheduler) warnStaleOutbox(ctx context.Context, cfg Config, now time.Time) {
	cutoff := now.Add(-cfg.OutboxStaleWarn)
	n, err := s.Store.CountStaleUnpublishedOutbox(ctx, cutoff)
	if err != nil {
		s.Log.Error().Err(err).Msg("reconciler: count stale outbox")
		return
	}
	if n > 0 {
		s.Log.Warn().Int("count", n).Dur("older_than", cfg.OutboxStaleWarn).Msg("reconciler: unpublished outbox for pending orders")
	}
}

func (s *Scheduler) rejectStalePending(ctx context.Context, cfg Config, now time.Time) {
	cutoff := now.Add(-cfg.PendingAcceptTimeout)
	orders, err := s.Store.FindStalePendingForReject(ctx, cutoff, cfg.BatchSize)
	if err != nil {
		s.Log.Error().Err(err).Msg("reconciler: find stale pending")
		return
	}
	for _, o := range orders {
		if s.Matching != nil {
			presence, err := s.Matching.GetOrderPresence(ctx, o.Symbol, o.ID)
			if err != nil {
				s.Log.Warn().Err(err).Uint64("order_id", o.ID).Str("symbol", o.Symbol).
					Msg("reconciler: matching presence failed, skip reject")
				continue
			}
			if !shouldRejectAfterMatching(presence) {
				s.Log.Info().Uint64("order_id", o.ID).Str("presence", presence.String()).
					Msg("reconciler: matching says order active/processed, skip reject")
				continue
			}
		}
		applied, err := s.Store.RejectStalePending(ctx, o.ID, cfg.CommandTopic)
		if err != nil {
			s.Log.Warn().Err(err).Uint64("order_id", o.ID).Msg("reconciler: reject pending failed")
			continue
		}
		if applied {
			s.Log.Warn().Uint64("order_id", o.ID).Msg("reconciler: pending timeout rejected, unfrozen, cancel enqueued")
		}
	}
}

func (s *Scheduler) resendStuckCancel(ctx context.Context, cfg Config, now time.Time) {
	cutoff := now.Add(-cfg.CancelConfirmTimeout)
	ids, err := s.Store.FindStuckCancelingForResend(ctx, cutoff, cfg.BatchSize)
	if err != nil {
		s.Log.Error().Err(err).Msg("reconciler: find stuck canceling")
		return
	}
	for _, id := range ids {
		sent, err := s.Store.ResendCancelCommand(ctx, id, cfg.CommandTopic)
		if err != nil {
			s.Log.Warn().Err(err).Uint64("order_id", id).Msg("reconciler: resend cancel failed")
			continue
		}
		if sent {
			s.Log.Warn().Uint64("order_id", id).Msg("reconciler: cancel command re-enqueued to outbox")
		}
	}
}

func (s *Scheduler) normalizedConfig() Config {
	cfg := s.Config
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.PendingAcceptTimeout <= 0 {
		cfg.PendingAcceptTimeout = 60 * time.Second
	}
	if cfg.CancelConfirmTimeout <= 0 {
		cfg.CancelConfirmTimeout = 30 * time.Second
	}
	if cfg.OutboxStaleWarn <= 0 {
		cfg.OutboxStaleWarn = 30 * time.Second
	}
	return cfg
}
