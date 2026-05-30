package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config 是 gateway 进程启动配置。
type Config struct {
	HTTPListen        string        `json:"http_listen"`
	OrderService      ServiceConfig `json:"order_service"`
	MarketDataService ServiceConfig `json:"marketdata_service"`
	KlineService      ServiceConfig               `json:"kline_service"`
	SymbolsFile       string                      `json:"symbols_file"`
	Symbols           map[string]SymbolRuleConfig `json:"symbols"`
	Auth              AuthConfig                  `json:"auth"`
	Log               LogConfig     `json:"log"`
}

type ServiceConfig struct {
	GRPCAddr    string `json:"grpc_addr"`
	GRPCDialSec int    `json:"dial_seconds"`
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
	return cfg, nil
}

func (c *Config) applyDefaults(raw map[string]json.RawMessage) {
	if c.HTTPListen == "" {
		c.HTTPListen = ":8080"
	}

	c.applyServiceDefaults(c.OrderService, "localhost:50051", 10)
	c.applyServiceDefaults(c.MarketDataService, "localhost:50052", 10)
	c.applyServiceDefaults(c.KlineService, "localhost:50053", 10)
	c.applySymbolDefaults()

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

func (c *Config) applyServiceDefaults(service ServiceConfig, defaultGRPCAddr string, defaultGRPCDialSec int) {
	if service.GRPCAddr == "" {
		service.GRPCAddr = defaultGRPCAddr
	}
	if service.GRPCDialSec <= 0 {
		service.GRPCDialSec = defaultGRPCDialSec
	}
}
