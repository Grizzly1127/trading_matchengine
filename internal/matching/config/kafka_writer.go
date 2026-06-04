package config

import (
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

// EventWriterConfig 将 Kafka 发布相关配置转为 EventWriter 参数。
func (k KafkaConfig) EventWriterConfig() kafka.WriterConfig {
	cfg := kafka.WriterConfig{
		Brokers:      k.Brokers,
		RequiredAcks: parseRequiredAcks(k.RequiredAcks),
		BatchSize:    k.BatchSize,
		Compression:  parseCompression(k.Compression),
	}
	if k.BatchTimeoutMs > 0 {
		cfg.BatchTimeout = time.Duration(k.BatchTimeoutMs) * time.Millisecond
	}
	return cfg
}

func parseRequiredAcks(s string) kafkago.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "one", "1":
		return kafkago.RequireOne
	case "none", "0":
		return kafkago.RequireNone
	case "all", "-1":
		return kafkago.RequireAll
	default:
		return kafkago.RequireAll
	}
}

func parseCompression(s string) kafkago.Compression {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "gzip":
		return kafkago.Gzip
	case "lz4":
		return kafkago.Lz4
	case "snappy":
		return kafkago.Snappy
	case "zstd":
		return kafkago.Zstd
	default:
		return 0
	}
}
