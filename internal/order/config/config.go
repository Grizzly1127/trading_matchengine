package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config 是 order 进程启动配置。
type Config struct {
	GRPCListen     string           `json:"grpc_listen"`
	DatabaseURL    string           `json:"database_url"`
	MigrateOnStart bool             `json:"migrate_on_start"`
	DefaultSymbol  string           `json:"default_symbol"`
	MarketData     MarketDataConfig `json:"marketdata"`
	Kafka          KafkaConfig      `json:"kafka"`
	Reconciler     ReconcilerConfig `json:"reconciler"`
	Log            LogConfig        `json:"log"`
}

type MarketDataConfig struct {
	GRPCAddr              string  `json:"grpc_addr"`
	DialTimeoutSeconds    int     `json:"dial_timeout_seconds"`
	RequestTimeoutSeconds int     `json:"request_timeout_seconds"`
	SlippageBuffer        float64 `json:"slippage_buffer"`
}

// ReconcilerConfig 超时补偿 scheduler（§4.5）。
type ReconcilerConfig struct {
	Enabled                     bool `json:"enabled"`
	IntervalSeconds             int  `json:"interval_seconds"`
	BatchSize                   int  `json:"batch_size"`
	PendingAcceptTimeoutSeconds int  `json:"pending_accept_timeout_seconds"`
	CancelConfirmTimeoutSeconds int  `json:"cancel_confirm_timeout_seconds"`
	OutboxStaleWarnSeconds      int  `json:"outbox_stale_warn_seconds"`
}

// KafkaConfig 控制 Outbox Relay 与事件消费。
type KafkaConfig struct {
	Brokers         []string `json:"brokers"`
	CommandTopic    string   `json:"command_topic"`
	MatchTopic      string   `json:"match_topic"`
	TradeTopic      string   `json:"trade_topic"`
	GroupID         string   `json:"group_id"`
	Partition       int      `json:"partition"`
	ConsumerEnabled bool     `json:"consumer_enabled"`
	// ConsumerStartOffset：-1 从最新；0 从最早（开发回放）。
	ConsumerStartOffset int64 `json:"consumer_start_offset"`
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
	if c.GRPCListen == "" {
		c.GRPCListen = ":50051"
	}
	if c.DefaultSymbol == "" {
		c.DefaultSymbol = "BTC-USDT"
	}
	if c.MarketData.GRPCAddr == "" {
		c.MarketData.GRPCAddr = "localhost:50052"
	}
	if c.MarketData.DialTimeoutSeconds <= 0 {
		c.MarketData.DialTimeoutSeconds = 3
	}
	if c.MarketData.RequestTimeoutSeconds <= 0 {
		c.MarketData.RequestTimeoutSeconds = 1
	}
	if c.MarketData.SlippageBuffer < 0 {
		c.MarketData.SlippageBuffer = 0
	}
	if _, ok := raw["migrate_on_start"]; !ok {
		c.MigrateOnStart = true
	}
	c.applyKafkaDefaults(raw)
	c.applyReconcilerDefaults(raw)
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if logRaw, ok := raw["log"]; ok {
		var logMap map[string]json.RawMessage
		if json.Unmarshal(logRaw, &logMap) == nil {
			if _, hasDev := logMap["dev"]; !hasDev {
				c.Log.Dev = true
			}
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
	}
}

func (c *Config) applyKafkaDefaults(raw map[string]json.RawMessage) {
	kafkaRaw, hasKafka := raw["kafka"]
	if c.Kafka.CommandTopic == "" {
		c.Kafka.CommandTopic = "order.commands"
	}
	if c.Kafka.MatchTopic == "" {
		c.Kafka.MatchTopic = "match.events"
	}
	if c.Kafka.TradeTopic == "" {
		c.Kafka.TradeTopic = "trade.events"
	}
	if c.Kafka.GroupID == "" {
		c.Kafka.GroupID = "order-service"
	}
	if hasKafka {
		var kafkaMap map[string]json.RawMessage
		if json.Unmarshal(kafkaRaw, &kafkaMap) == nil {
			if _, ok := kafkaMap["consumer_enabled"]; !ok {
				c.Kafka.ConsumerEnabled = true
			}
			if _, ok := kafkaMap["consumer_start_offset"]; !ok && c.Kafka.ConsumerStartOffset == 0 {
				c.Kafka.ConsumerStartOffset = -1
			}
		}
	} else if c.Kafka.ConsumerStartOffset == 0 {
		c.Kafka.ConsumerStartOffset = -1
	}
}

func (c *Config) applyReconcilerDefaults(raw map[string]json.RawMessage) {
	reconcilerRaw, has := raw["reconciler"]
	if !has {
		c.Reconciler.Enabled = true
	}
	if c.Reconciler.IntervalSeconds <= 0 {
		c.Reconciler.IntervalSeconds = 60
	}
	if c.Reconciler.BatchSize <= 0 {
		c.Reconciler.BatchSize = 50
	}
	if c.Reconciler.PendingAcceptTimeoutSeconds <= 0 {
		c.Reconciler.PendingAcceptTimeoutSeconds = 60
	}
	if c.Reconciler.CancelConfirmTimeoutSeconds <= 0 {
		c.Reconciler.CancelConfirmTimeoutSeconds = 30
	}
	if c.Reconciler.OutboxStaleWarnSeconds <= 0 {
		c.Reconciler.OutboxStaleWarnSeconds = 30
	}
	if has {
		var m map[string]json.RawMessage
		if json.Unmarshal(reconcilerRaw, &m) == nil {
			if _, ok := m["enabled"]; !ok {
				c.Reconciler.Enabled = true
			}
		}
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("config: database_url is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("config: kafka.brokers is required")
	}
	if strings.TrimSpace(c.Kafka.CommandTopic) == "" {
		return fmt.Errorf("config: kafka.command_topic is required")
	}
	if c.Kafka.ConsumerEnabled {
		if strings.TrimSpace(c.Kafka.MatchTopic) == "" {
			return fmt.Errorf("config: kafka.match_topic is required when consumer_enabled")
		}
		if strings.TrimSpace(c.Kafka.TradeTopic) == "" {
			return fmt.Errorf("config: kafka.trade_topic is required when consumer_enabled")
		}
		if strings.TrimSpace(c.Kafka.GroupID) == "" {
			return fmt.Errorf("config: kafka.group_id is required when consumer_enabled")
		}
	}
	return nil
}
