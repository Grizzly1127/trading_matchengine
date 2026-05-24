package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config 是 order 进程启动配置。
type Config struct {
	GRPCListen     string      `json:"grpc_listen"`
	DatabaseURL    string      `json:"database_url"`
	MigrateOnStart bool        `json:"migrate_on_start"`
	DefaultSymbol  string      `json:"default_symbol"`
	Kafka          KafkaConfig `json:"kafka"`
	Log            LogConfig   `json:"log"`
}

// KafkaConfig 控制向 order.commands 直接发布（第 4 步 4.1，尚未 Outbox）。
type KafkaConfig struct {
	Brokers      []string `json:"brokers"`
	CommandTopic string   `json:"command_topic"`
	Partition    int      `json:"partition"`
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
	if _, ok := raw["migrate_on_start"]; !ok {
		c.MigrateOnStart = true
	}
	if c.Kafka.CommandTopic == "" {
		c.Kafka.CommandTopic = "order.commands"
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
	return nil
}
