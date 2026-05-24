package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config 是 matching 进程启动配置。
type Config struct {
	DataDir        string      `json:"data_dir"`
	ShardID        string      `json:"shard_id"`
	SnapshotEvery  uint64      `json:"snapshot_every"`
	SnapshotOnExit bool        `json:"snapshot_on_exit"`
	CommandsFile   string      `json:"commands_file"`
	DefaultSymbol  string      `json:"default_symbol"`
	Kafka          KafkaConfig `json:"kafka"`
	Log            LogConfig   `json:"log"`
}

// KafkaConfig 控制 Kafka 消费与发布（3.2）。
type KafkaConfig struct {
	Enabled      bool     `json:"enabled"`
	Brokers      []string `json:"brokers"`
	GroupID      string   `json:"group_id"`
	CommandTopic string   `json:"command_topic"`
	MatchTopic   string   `json:"match_topic"`
	TradeTopic   string   `json:"trade_topic"`
	Partition    int      `json:"partition"`
}

// LogConfig 控制结构化日志。
type LogConfig struct {
	Level        string `json:"level"`
	Dev          bool   `json:"dev"`
	File         string `json:"file"`
	Async        bool   `json:"async"`
	BufferSize   int    `json:"buffer_size"`
	MaxSizeMB    int    `json:"max_size_mb"`
	MaxAgeDays   int    `json:"max_age_days"`
	MaxBackups   int    `json:"max_backups"`
	Compress     bool   `json:"compress"`
	LocalTime    bool   `json:"local_time"`
	RotateDaily  bool   `json:"rotate_daily"`
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
	cfg.applyKafkaDefaults(raw)
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
}
