package outbox

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Store Outbox Relay 所需的持久化接口。
type Store interface {
	ClaimUnpublished(ctx context.Context, limit int) (ClaimHandle, error)
	MarkPublished(ctx context.Context, id uint64) error
	MarkPublishedBatch(ctx context.Context, ids []uint64) error
	IncrementRetry(ctx context.Context, id uint64) error
	GetOrderStatus(ctx context.Context, orderID uint64) (string, error)
	GetOrderStatusesBatch(ctx context.Context, orderIDs []uint64) (map[uint64]string, error)
}

// KafkaWriter 发布 Outbox 消息到 Kafka。
type KafkaWriter interface {
	WriteAt(ctx context.Context, topic string, partition int, key, value []byte) error
	WriteBatchAt(ctx context.Context, topic string, partition int, key []byte, values [][]byte) error
}

// PartitionResolver 按 symbol（partition_key）解析 Kafka 分区；由 Shard Manager 实现。
type PartitionResolver interface {
	PartitionForSymbol(symbol string) (int, error)
}

// RelayConfig 控制后台投递行为。
type RelayConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetry     int
	Workers      int
	// Partition 为未配置 Resolver 时的回退分区。
	Partition int
	Resolver  PartitionResolver
}

// RelayMetrics Relay 投递可观测性（可选注入）。
type RelayMetrics interface {
	ObserveRelayBatchSize(n int)
	ObserveRelayDispatchLatencyMs(ms float64)
	AddRelayPublished(n int)
}

// Relay 轮询 order_outbox 并投递至 Kafka。
type Relay struct {
	Store   Store
	Writer  KafkaWriter
	Log     zerolog.Logger
	Config  RelayConfig
	Metrics RelayMetrics
}

// Run 启动 Workers 个 goroutine，阻塞直至 ctx 取消。
func (r *Relay) Run(ctx context.Context) {
	if r == nil || r.Store == nil || r.Writer == nil {
		return
	}
	cfg := r.normalizedConfig()
	workers := cfg.Workers
	if workers <= 1 {
		r.runWorker(ctx, cfg, 0)
		return
	}
	var wg sync.WaitGroup
	for workerID := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.runWorker(ctx, cfg, id)
		}(workerID)
	}
	wg.Wait()
}

func (r *Relay) runWorker(ctx context.Context, cfg RelayConfig, workerID int) {
	log := r.Log
	if workerID > 0 || cfg.Workers > 1 {
		log = log.With().Int("worker", workerID).Logger()
	}
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		n := r.pollOnce(ctx, cfg, log)
		if n >= cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pollOnce 领取并投递一批；返回本批条数（供 runWorker 判断是否连续 poll）。
func (r *Relay) pollOnce(ctx context.Context, cfg RelayConfig, log zerolog.Logger) int {
	start := time.Now()
	claim, err := r.Store.ClaimUnpublished(ctx, cfg.BatchSize)
	if err != nil {
		log.Error().Err(err).Msg("outbox relay: claim unpublished")
		return 0
	}
	defer func() { _ = claim.Rollback(ctx) }()

	entries := claim.Entries()
	if len(entries) == 0 {
		return 0
	}
	published, err := r.dispatchBatch(ctx, cfg, claim, entries, log)
	if err != nil {
		log.Warn().Err(err).Msg("outbox relay: dispatch batch")
		r.recordRelayMetrics(len(entries), start, published)
		return len(entries)
	}
	if err := claim.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("outbox relay: commit claim")
		r.recordRelayMetrics(len(entries), start, published)
		return len(entries)
	}
	r.recordRelayMetrics(len(entries), start, published)
	return len(entries)
}

func (r *Relay) recordRelayMetrics(batchSize int, start time.Time, published int) {
	if r == nil || r.Metrics == nil {
		return
	}
	r.Metrics.ObserveRelayBatchSize(batchSize)
	r.Metrics.ObserveRelayDispatchLatencyMs(float64(time.Since(start).Milliseconds()))
	r.Metrics.AddRelayPublished(published)
}

type relayGroupKey struct {
	topic     string
	partition int
	key       string
}

type relayPublishItem struct {
	entry Entry
}

func (r *Relay) dispatchBatch(ctx context.Context, cfg RelayConfig, claim ClaimHandle, entries []Entry, log zerolog.Logger) (int, error) {
	skipIDs := make([]uint64, 0, len(entries))
	groups := make(map[relayGroupKey][]relayPublishItem, len(entries))
	published := 0

	orderIDs := make([]uint64, len(entries))
	for i, e := range entries {
		orderIDs[i] = e.AggregateID
	}
	statuses, err := r.Store.GetOrderStatusesBatch(ctx, orderIDs)
	if err != nil {
		return 0, fmt.Errorf("get order statuses batch: %w", err)
	}

	for _, e := range entries {
		status, ok := statuses[e.AggregateID]
		if !ok {
			log.Warn().Uint64("outbox_id", e.ID).Uint64("order_id", e.AggregateID).Msg("outbox relay: order not found")
			continue
		}
		if !isSendableStatus(status) {
			skipIDs = append(skipIDs, e.ID)
			continue
		}
		if e.RetryCount >= cfg.MaxRetry {
			log.Warn().Uint64("outbox_id", e.ID).Int("retry", e.RetryCount).Msg("outbox relay: max retry exceeded")
			continue
		}

		partition, err := r.resolvePartition(cfg, e.PartitionKey)
		if err != nil {
			log.Warn().Err(err).Uint64("outbox_id", e.ID).Msg("outbox relay: resolve partition")
			continue
		}
		k := relayGroupKey{topic: e.Topic, partition: partition, key: e.PartitionKey}
		groups[k] = append(groups[k], relayPublishItem{entry: e})
	}

	if len(skipIDs) > 0 {
		if err := claim.MarkPublishedBatch(ctx, skipIDs); err != nil {
			return published, fmt.Errorf("mark skipped terminal: %w", err)
		}
		published += len(skipIDs)
	}

	for k, items := range groups {
		n, err := r.publishGroup(ctx, claim, k.topic, k.partition, k.key, items)
		if err != nil {
			log.Warn().Err(err).Str("topic", k.topic).Int("partition", k.partition).Msg("outbox relay: publish group failed")
			continue
		}
		published += n
	}
	return published, nil
}

func (r *Relay) publishGroup(ctx context.Context, claim ClaimHandle, topic string, partition int, key string, items []relayPublishItem) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	values := make([][]byte, len(items))
	ids := make([]uint64, len(items))
	for i, it := range items {
		values[i] = it.entry.Payload
		ids[i] = it.entry.ID
	}

	if err := r.Writer.WriteBatchAt(ctx, topic, partition, []byte(key), values); err != nil {
		for _, id := range ids {
			if incErr := r.Store.IncrementRetry(ctx, id); incErr != nil {
				return 0, fmt.Errorf("kafka write: %w; increment retry %d: %v", err, id, incErr)
			}
		}
		return 0, err
	}
	if err := claim.MarkPublishedBatch(ctx, ids); err != nil {
		return 0, err
	}
	return len(ids), nil
}

// dispatchOne 单条投递（测试与兼容）。
func (r *Relay) dispatchOne(ctx context.Context, cfg RelayConfig, e Entry) error {
	claim, err := r.Store.ClaimUnpublished(ctx, 1)
	if err != nil {
		return err
	}
	defer func() { _ = claim.Rollback(ctx) }()
	entries := claim.Entries()
	if len(entries) == 0 {
		return nil
	}
	if _, err := r.dispatchBatch(ctx, cfg, claim, entries, r.Log); err != nil {
		return err
	}
	return claim.Commit(ctx)
}

func isSendableStatus(status string) bool {
	return status == "PENDING" || status == "CANCELING"
}

func (r *Relay) resolvePartition(cfg RelayConfig, partitionKey string) (int, error) {
	if r != nil && cfg.Resolver != nil {
		return cfg.Resolver.PartitionForSymbol(partitionKey)
	}
	return cfg.Partition, nil
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
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Partition < 0 {
		cfg.Partition = 0
	}
	return cfg
}
