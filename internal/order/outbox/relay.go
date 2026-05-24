package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

// Entry 待投递 Outbox 行。
type Entry struct {
	ID           uint64
	AggregateID  uint64
	EventType    string
	Payload      []byte
	Topic        string
	PartitionKey string
	RetryCount   int
}

// Store Outbox Relay 所需的持久化接口。
type Store interface {
	FetchUnpublished(ctx context.Context, limit int) ([]Entry, error)
	MarkPublished(ctx context.Context, id uint64) error
	IncrementRetry(ctx context.Context, id uint64) error
	GetOrderStatus(ctx context.Context, orderID uint64) (string, error)
}

// KafkaWriter 发布 Outbox 消息到 Kafka。
type KafkaWriter interface {
	WriteAt(ctx context.Context, topic string, partition int, key, value []byte) error
}

// RelayConfig 控制后台投递行为。
type RelayConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetry     int
	Partition    int
}

// Relay 轮询 order_outbox 并投递至 Kafka。
type Relay struct {
	Store  Store
	Writer KafkaWriter
	Log    zerolog.Logger
	Config RelayConfig
}

// Run 阻塞运行直至 ctx 取消。
func (r *Relay) Run(ctx context.Context) {
	if r == nil || r.Store == nil || r.Writer == nil {
		return
	}
	cfg := r.normalizedConfig()
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.pollOnce(ctx, cfg)
		}
	}
}

func (r *Relay) pollOnce(ctx context.Context, cfg RelayConfig) {
	entries, err := r.Store.FetchUnpublished(ctx, cfg.BatchSize)
	if err != nil {
		r.Log.Error().Err(err).Msg("outbox relay: fetch unpublished")
		return
	}
	for _, e := range entries {
		if err := r.dispatchOne(ctx, cfg, e); err != nil {
			r.Log.Warn().Err(err).Uint64("outbox_id", e.ID).Msg("outbox relay: dispatch failed")
		}
	}
}

func (r *Relay) dispatchOne(ctx context.Context, cfg RelayConfig, e Entry) error {
	status, err := r.Store.GetOrderStatus(ctx, e.AggregateID)
	if err != nil {
		return err
	}
	if !isSendableStatus(status) {
		return r.Store.MarkPublished(ctx, e.ID)
	}
	if e.RetryCount >= cfg.MaxRetry {
		return fmt.Errorf("outbox %d exceeded max_retry=%d", e.ID, cfg.MaxRetry)
	}

	if err := r.Writer.WriteAt(ctx, e.Topic, cfg.Partition, []byte(e.PartitionKey), e.Payload); err != nil {
		if incErr := r.Store.IncrementRetry(ctx, e.ID); incErr != nil {
			return fmt.Errorf("kafka write: %w; increment retry: %v", err, incErr)
		}
		return err
	}
	return r.Store.MarkPublished(ctx, e.ID)
}

func isSendableStatus(status string) bool {
	return status == "PENDING" || status == "CANCELING"
}

func (r *Relay) normalizedConfig() RelayConfig {
	cfg := r.Config
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 100
	}
	if cfg.Partition < 0 {
		cfg.Partition = 0
	}
	return cfg
}
