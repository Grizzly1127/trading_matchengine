package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/aggregator"
)

// Config Index Price Service 配置。
type Config struct {
	GRPCListen      string          `json:"grpc_listen"`
	DatabaseURL     string          `json:"database_url"`
	Symbols         []string        `json:"symbols"`
	PollIntervalMs  int             `json:"poll_interval_ms"`
	FetchTimeoutMs  int             `json:"fetch_timeout_ms"`
	StaleAfterMs    int             `json:"stale_after_ms"`
	RedisKeyTTLMs   int             `json:"redis_key_ttl_ms"`
	Aggregator      AggregatorConfig `json:"aggregator"`
	Sources         SourcesConfig   `json:"sources"`
	Mock            MockConfig      `json:"mock"`
	Kafka           KafkaConfig     `json:"kafka"`
	Redis           RedisConfig     `json:"redis"`
	Log             LogConfig       `json:"log"`
}

type AggregatorConfig struct {
	DeviationThreshold string `json:"deviation_threshold"`
	MinSources         int    `json:"min_sources"`
}

type SourceConfig struct {
	Enabled bool   `json:"enabled"`
	Weight  string `json:"weight"`
	BaseURL string `json:"base_url"`
}

type SourcesConfig struct {
	Mock    SourceConfig `json:"mock"`
	Binance SourceConfig `json:"binance"`
	OKX     SourceConfig `json:"okx"`
	Bybit   SourceConfig `json:"bybit"`
}

type MockConfig struct {
	BasePrices  map[string]string `json:"base_prices"`
	DefaultBase string            `json:"default_base"`
}

type KafkaConfig struct {
	Brokers          []string `json:"brokers"`
	IndexTopic       string   `json:"index_topic"`
	ProducerEnabled  bool     `json:"producer_enabled"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

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

// Load 从 JSON 文件加载配置。
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
		c.GRPCListen = ":50054"
	}
	if c.PollIntervalMs <= 0 {
		c.PollIntervalMs = 1000
	}
	if c.FetchTimeoutMs <= 0 {
		c.FetchTimeoutMs = 800
	}
	if c.StaleAfterMs <= 0 {
		c.StaleAfterMs = 60000
	}
	if c.RedisKeyTTLMs <= 0 {
		c.RedisKeyTTLMs = 10000
	}
	if c.Aggregator.DeviationThreshold == "" {
		c.Aggregator.DeviationThreshold = "0.03"
	}
	if c.Aggregator.MinSources <= 0 {
		c.Aggregator.MinSources = 1
	}
	if len(c.Symbols) == 0 {
		c.Symbols = []string{"BTC-USDT"}
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "localhost:6379"
	}
	if c.Kafka.IndexTopic == "" {
		c.Kafka.IndexTopic = "index.price"
	}
	if kafkaRaw, ok := raw["kafka"]; ok {
		var kafkaMap map[string]json.RawMessage
		if json.Unmarshal(kafkaRaw, &kafkaMap) == nil {
			if _, ok := kafkaMap["producer_enabled"]; !ok {
				c.Kafka.ProducerEnabled = true
			}
		}
	} else {
		c.Kafka.ProducerEnabled = true
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
		}
	} else {
		c.Log.Dev = true
	}
	if c.Log.BufferSize <= 0 {
		c.Log.BufferSize = 512
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("config: database_url is required")
	}
	if len(c.Kafka.Brokers) == 0 && c.Kafka.ProducerEnabled {
		return fmt.Errorf("config: kafka.brokers is required when producer_enabled")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("config: redis.addr is required")
	}
	if _, err := aggregator.ParseDeviation(c.Aggregator.DeviationThreshold); err != nil {
		return err
	}
	return nil
}

// AggregatorParams 返回聚合器运行参数。
func (c Config) AggregatorParams() (aggregator.Config, error) {
	dev, err := aggregator.ParseDeviation(c.Aggregator.DeviationThreshold)
	if err != nil {
		return aggregator.Config{}, err
	}
	return aggregator.Config{
		DeviationThreshold: dev,
		MinSources:         c.Aggregator.MinSources,
	}, nil
}
