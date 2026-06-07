package config

import (
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

// OutboxRelayConfig Outbox Relay 轮询与批量投递参数。
type OutboxRelayConfig struct {
	PollIntervalMs int `json:"poll_interval_ms"`
	BatchSize      int `json:"batch_size"`
	MaxRetry       int `json:"max_retry"`
	Workers        int `json:"workers"`
}

// OutboxRelayRuntime 将配置转为 Relay 运行时参数。
func (c Config) OutboxRelayRuntime(partition int, resolver outbox.PartitionResolver) outbox.RelayConfig {
	rc := c.OutboxRelay
	return outbox.RelayConfig{
		PollInterval: time.Duration(rc.PollIntervalMs) * time.Millisecond,
		BatchSize:    rc.BatchSize,
		MaxRetry:     rc.MaxRetry,
		Workers:      rc.Workers,
		Partition:    partition,
		Resolver:     resolver,
	}
}

// KafkaWriterConfig 构建 Outbox 使用的 Kafka EventWriter 配置。
func (c Config) KafkaWriterConfig() kafka.WriterConfig {
	k := c.Kafka
	wc := kafka.WriterConfig{Brokers: k.Brokers}
	if k.BatchSize > 0 {
		wc.BatchSize = k.BatchSize
	}
	if k.BatchTimeoutMs > 0 {
		wc.BatchTimeout = time.Duration(k.BatchTimeoutMs) * time.Millisecond
	}
	return wc
}
