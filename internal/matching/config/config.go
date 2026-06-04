package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// WALGroupCommitConfig WAL 组提交
type WALGroupCommitConfig struct {
	SyncEveryRecords    int `json:"sync_every_records"`     // <=1：每条 fsync（默认）
	SyncIntervalMs      int `json:"sync_interval_ms"`       // 与 sync_every_records>1 配合
	ConsumerBatchMax    int `json:"consumer_batch_max"`     // Kafka 凑批上限，0=与 sync_every_records 对齐
	ConsumerBatchWaitMs int `json:"consumer_batch_wait_ms"` // 凑批最长等待毫秒
}

// Config 是 matching 进程启动配置。
type Config struct {
	DataDir         string                      `json:"data_dir"`
	ShardID         string                      `json:"shard_id"`
	ShardsFile      string                      `json:"shards_file"`
	SnapshotEvery   uint64                      `json:"snapshot_every"`
	SnapshotOnExit  bool                        `json:"snapshot_on_exit"`
	WALGroupCommit  WALGroupCommitConfig        `json:"wal_group_commit"`
	CommandsFile    string                      `json:"commands_file"`
	DefaultSymbol   string                      `json:"default_symbol"`
	SymbolsFile     string                      `json:"symbols_file"`
	Symbols         map[string]SymbolRuleConfig `json:"symbols"`
	MetricsListen   string                      `json:"metrics_listen"`
	AdminGRPCListen string                      `json:"admin_grpc_listen"`
	OrderService    OrderServiceConfig          `json:"order_service"`
	Kafka           KafkaConfig                 `json:"kafka"`
	Log             LogConfig                   `json:"log"`
}

// OrderServiceConfig 启动对账用 Order Admin gRPC。
type OrderServiceConfig struct {
	Enabled                      bool   `json:"enabled"`
	GRPCAddr                     string `json:"grpc_addr"`
	DialTimeoutSeconds           int    `json:"dial_timeout_seconds"`
	RecoveryVerifyTimeoutSeconds int    `json:"recovery_verify_timeout_seconds"`
}

// KafkaConfig 控制 Kafka 消费与发布。
type KafkaConfig struct {
	Enabled      bool     `json:"enabled"`
	Brokers      []string `json:"brokers"`
	GroupID      string   `json:"group_id"`
	CommandTopic string   `json:"command_topic"`
	MatchTopic   string   `json:"match_topic"`
	TradeTopic   string   `json:"trade_topic"`
	Partition    int      `json:"partition"`
	// 发布 match/trade 事件（见 EventWriterConfig）；未配置时 acks=all、batch 见 pkg/kafka 默认。
	RequiredAcks   string `json:"required_acks"`    // all | one | none
	BatchSize      int    `json:"batch_size"`       // 0 = 默认 100
	BatchTimeoutMs int    `json:"batch_timeout_ms"` // 0 = 默认 10ms
	Compression    string `json:"compression"`      // 空 | gzip | lz4 | snappy | zstd
}

// LogConfig 控制结构化日志。
type LogConfig struct {
	Level       string `json:"level"`
	Dev         bool   `json:"dev"`
	File        string `json:"file"`
	Async       bool   `json:"async"`
	BufferSize  int    `json:"buffer_size"`
	MaxSizeMB   int    `json:"max_size_mb"`
	MaxAgeDays  int    `json:"max_age_days"`
	MaxBackups  int    `json:"max_backups"`
	Compress    bool   `json:"compress"`
	LocalTime   bool   `json:"local_time"`
	RotateDaily bool   `json:"rotate_daily"`
}

// Load 从 JSON 文件加载配置并填充默认值。
func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, fmt.Errorf("config: path is required")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %q: %w", path, err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return Config{}, fmt.Errorf("config: parse %q: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse %q: %w", path, err)
	}

	cfg.applyDefaults(raw)
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults(raw map[string]json.RawMessage) {
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	if _, ok := raw["shard_id"]; !ok {
		if c.ShardID == "" {
			c.ShardID = "shard-0"
		}
	}
	if c.SnapshotEvery == 0 {
		c.SnapshotEvery = 10000
	}
	if _, ok := raw["snapshot_on_exit"]; !ok {
		c.SnapshotOnExit = true
	}
	if c.DefaultSymbol == "" {
		c.DefaultSymbol = "BTC-USDT"
	}
	if _, ok := raw["metrics_listen"]; !ok {
		if c.MetricsListen == "" {
			c.MetricsListen = ":9101"
		}
	}
	if _, ok := raw["admin_grpc_listen"]; !ok {
		if c.AdminGRPCListen == "" {
			c.AdminGRPCListen = ":50061"
		}
	}
	c.applyKafkaDefaults(raw)
	c.applyWALGroupCommitDefaults(raw)
	c.applyOrderServiceDefaults(raw)
	c.applySymbolDefaults()
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if logRaw, ok := raw["log"]; ok {
		var logMap map[string]json.RawMessage
		if json.Unmarshal(logRaw, &logMap) == nil {
			if _, hasDev := logMap["dev"]; !hasDev {
				c.Log.Dev = true
			}
			// 配置了 file 且未写 async 时，默认异步落盘
			if _, hasFile := logMap["file"]; hasFile && c.Log.File != "" {
				if _, hasAsync := logMap["async"]; !hasAsync {
					c.Log.Async = true
				}
			}
		}
	} else {
		c.Log.Dev = true
	}
	if c.Log.BufferSize <= 0 {
		c.Log.BufferSize = 512
	}
	if c.Log.File != "" {
		if c.Log.MaxSizeMB <= 0 {
			c.Log.MaxSizeMB = 100
		}
		if c.Log.MaxAgeDays <= 0 {
			c.Log.MaxAgeDays = 7
		}
		if logRaw, ok := raw["log"]; ok {
			var logMap map[string]json.RawMessage
			if json.Unmarshal(logRaw, &logMap) == nil {
				if _, has := logMap["rotate_daily"]; !has {
					c.Log.RotateDaily = true
				}
				if _, has := logMap["local_time"]; !has {
					c.Log.LocalTime = true
				}
			}
		}
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.ShardID) == "" {
		return fmt.Errorf("config: shard_id is required")
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("config: data_dir is required")
	}
	if c.Kafka.Enabled {
		if len(c.Kafka.Brokers) == 0 {
			return fmt.Errorf("config: kafka.brokers is required when kafka.enabled")
		}
		if strings.TrimSpace(c.Kafka.GroupID) == "" {
			return fmt.Errorf("config: kafka.group_id is required when kafka.enabled")
		}
		if strings.TrimSpace(c.Kafka.CommandTopic) == "" {
			return fmt.Errorf("config: kafka.command_topic is required when kafka.enabled")
		}
		if strings.TrimSpace(c.Kafka.MatchTopic) == "" {
			return fmt.Errorf("config: kafka.match_topic is required when kafka.enabled")
		}
		if strings.TrimSpace(c.Kafka.TradeTopic) == "" {
			return fmt.Errorf("config: kafka.trade_topic is required when kafka.enabled")
		}
	}
	return nil
}

func (c *Config) applyOrderServiceDefaults(raw map[string]json.RawMessage) {
	if _, ok := raw["order_service"]; !ok {
		c.OrderService.Enabled = true
		if c.OrderService.GRPCAddr == "" {
			c.OrderService.GRPCAddr = "localhost:50051"
		}
	}
	if c.OrderService.DialTimeoutSeconds <= 0 {
		c.OrderService.DialTimeoutSeconds = 3
	}
	if c.OrderService.RecoveryVerifyTimeoutSeconds <= 0 {
		c.OrderService.RecoveryVerifyTimeoutSeconds = 30
	}
}

func (c *Config) applyWALGroupCommitDefaults(raw map[string]json.RawMessage) {
	if c.WALGroupCommit.SyncEveryRecords <= 0 {
		c.WALGroupCommit.SyncEveryRecords = 1
	}
	if c.WALGroupCommit.GroupCommitEnabled() {
		if c.WALGroupCommit.ConsumerBatchMax <= 0 {
			c.WALGroupCommit.ConsumerBatchMax = c.WALGroupCommit.SyncEveryRecords
		}
		if c.WALGroupCommit.ConsumerBatchWaitMs <= 0 {
			c.WALGroupCommit.ConsumerBatchWaitMs = 2
		}
	}
	_ = raw
}

// GroupCommitEnabled 是否启用 WAL 组提交。
func (c WALGroupCommitConfig) GroupCommitEnabled() bool {
	return c.SyncEveryRecords > 1 || c.SyncIntervalMs > 0
}

// ConsumerRunOptions 转为 consumer.RunOptions。
func (c WALGroupCommitConfig) ConsumerRunOptions() (batchMax int, batchWait time.Duration) {
	batchMax = 1
	if !c.GroupCommitEnabled() {
		return batchMax, 0
	}
	batchMax = c.ConsumerBatchMax
	if batchMax <= 0 {
		batchMax = c.SyncEveryRecords
	}
	return batchMax, time.Duration(c.ConsumerBatchWaitMs) * time.Millisecond
}

func (c *Config) applyKafkaDefaults(raw map[string]json.RawMessage) {
	if _, ok := raw["kafka"]; !ok {
		return
	}
	if c.Kafka.GroupID == "" && c.Kafka.Enabled {
		c.Kafka.GroupID = "matching-" + c.ShardID
	}
	if c.Kafka.CommandTopic == "" {
		c.Kafka.CommandTopic = "order.commands"
	}
	if c.Kafka.MatchTopic == "" {
		c.Kafka.MatchTopic = "match.events"
	}
	if c.Kafka.TradeTopic == "" {
		c.Kafka.TradeTopic = "trade.events"
	}
	if c.Kafka.BatchSize <= 0 {
		c.Kafka.BatchSize = 100
	}
	if c.Kafka.BatchTimeoutMs <= 0 {
		c.Kafka.BatchTimeoutMs = 10
	}
}
