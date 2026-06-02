package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	GRPCListen      string      `json:"grpc_listen"`
	MetricsListen   string      `json:"metrics_listen"`
	DatabaseURL     string      `json:"database_url"`
	ClosedQueueSize int         `json:"closed_queue_size"`
	Kafka           KafkaConfig `json:"kafka"`
	Redis           RedisConfig `json:"redis"`
	Log             LogConfig   `json:"log"`
}

// KafkaConfig 控制 Outbox Relay 与事件消费。
type KafkaConfig struct {
	Brokers         []string `json:"brokers"`
	TradeTopic      string   `json:"trade_topic"`
	KlineRawTopic   string   `json:"kline_raw_topic"`
	GroupID         string   `json:"group_id"`
	Partition       int      `json:"partition"`
	ConsumerEnabled bool     `json:"consumer_enabled"`
	ProducerEnabled bool     `json:"producer_enabled"`
	// ConsumerStartOffset：-1 从最新；0 从最早（开发回放）。
	ConsumerStartOffset int64 `json:"consumer_start_offset"`
}

// RedisConfig 控制 Redis 缓存。
type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
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
		c.GRPCListen = ":50053"
	}
	if c.MetricsListen == "" {
		c.MetricsListen = ":9105"
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "localhost:6379"
	}
	if c.Redis.Password == "" {
		c.Redis.Password = ""
	}
	if c.Redis.DB == 0 {
		c.Redis.DB = 0
	}
	c.applyKafkaDefaults(raw)
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
	if c.Kafka.TradeTopic == "" {
		c.Kafka.TradeTopic = "trade.events"
	}
	if c.Kafka.KlineRawTopic == "" {
		c.Kafka.KlineRawTopic = "kline.raw"
	}
	if c.Kafka.GroupID == "" {
		c.Kafka.GroupID = "kline-service"
	}
	if hasKafka {
		var kafkaMap map[string]json.RawMessage
		if json.Unmarshal(kafkaRaw, &kafkaMap) == nil {
			if _, ok := kafkaMap["consumer_enabled"]; !ok {
				c.Kafka.ConsumerEnabled = true
			}
			if _, ok := kafkaMap["producer_enabled"]; !ok {
				c.Kafka.ProducerEnabled = true
			}
			if _, ok := kafkaMap["consumer_start_offset"]; !ok && c.Kafka.ConsumerStartOffset == 0 {
				c.Kafka.ConsumerStartOffset = -1
			}
		}
	} else {
		c.Kafka.ProducerEnabled = true
		if c.Kafka.ConsumerStartOffset == 0 {
			c.Kafka.ConsumerStartOffset = -1
		}
	}
}

func (c Config) validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("config: kafka.brokers is required")
	}
	if c.Kafka.ConsumerEnabled {
		if strings.TrimSpace(c.Kafka.TradeTopic) == "" {
			return fmt.Errorf("config: kafka.trade_topic is required when consumer_enabled")
		}
		if strings.TrimSpace(c.Kafka.GroupID) == "" {
			return fmt.Errorf("config: kafka.group_id is required when consumer_enabled")
		}
	}
	if c.Kafka.ProducerEnabled && strings.TrimSpace(c.Kafka.KlineRawTopic) == "" {
		return fmt.Errorf("config: kafka.kline_raw_topic is required when producer_enabled")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("config: redis.addr is required")
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("config: database_url is required")
	}
	if c.ClosedQueueSize <= 0 {
		c.ClosedQueueSize = 4096
	}
	return nil
}
