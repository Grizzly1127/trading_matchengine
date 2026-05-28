package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config 是 gateway 进程启动配置。
type Config struct {
	HTTPListen            string      `json:"http_listen"`
	OrderGRPCAddr         string      `json:"order_grpc_addr"`
	OrderGRPCDialSec      int         `json:"order_grpc_dial_seconds"`
	MarketDataGRPCAddr    string      `json:"marketdata_grpc_addr"`
	MarketDataGRPCDialSec int         `json:"marketdata_grpc_dial_seconds"`
	Redis                 RedisConfig `json:"redis"`
	Auth                  AuthConfig  `json:"auth"`
	Log                   LogConfig   `json:"log"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// AuthConfig Phase 1 静态 Bearer 鉴权（用户 ID 由请求传入，见 rest-api §2.2）。
type AuthConfig struct {
	StaticToken string `json:"static_token"`
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
	if c.HTTPListen == "" {
		c.HTTPListen = ":8080"
	}
	if c.OrderGRPCAddr == "" {
		c.OrderGRPCAddr = "localhost:50051"
	}
	if c.OrderGRPCDialSec <= 0 {
		c.OrderGRPCDialSec = 10
	}
	if c.MarketDataGRPCAddr == "" {
		c.MarketDataGRPCAddr = "localhost:50052"
	}
	if c.MarketDataGRPCDialSec <= 0 {
		c.MarketDataGRPCDialSec = 10
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "localhost:6379"
	}
	if c.Auth.StaticToken == "" {
		c.Auth.StaticToken = "dev-token-change-me"
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
	if strings.TrimSpace(c.HTTPListen) == "" {
		return fmt.Errorf("config: http_listen is required")
	}
	if strings.TrimSpace(c.OrderGRPCAddr) == "" {
		return fmt.Errorf("config: order_grpc_addr is required")
	}
	if strings.TrimSpace(c.MarketDataGRPCAddr) == "" {
		return fmt.Errorf("config: marketdata_grpc_addr is required")
	}
	if strings.TrimSpace(c.Auth.StaticToken) == "" {
		return fmt.Errorf("config: auth.static_token is required")
	}
	return nil
}
