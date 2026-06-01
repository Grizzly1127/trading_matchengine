package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
)

type Config struct {
	HTTPListen string         `json:"http_listen"`
	Auth       auth.Config    `json:"auth"`
	TLS        auth.TLSConfig `json:"tls"`
	Redis      RedisConfig    `json:"redis"`
	Log        LogConfig      `json:"log"`
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

func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, fmt.Errorf("config: path is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse %q: %w", path, err)
	}
	if cfg.HTTPListen == "" {
		cfg.HTTPListen = ":8081"
	}
	if strings.TrimSpace(cfg.Auth.Mode) == "" {
		cfg.Auth.Mode = "static"
	}
	if cfg.Auth.Mode == "static" && cfg.Auth.StaticToken == "" {
		cfg.Auth.StaticToken = "dev-token-change-me"
	}
	if cfg.Redis.Addr == "" {
		cfg.Redis.Addr = "localhost:6379"
	}
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info"
	}
	if !cfg.Log.Dev {
		cfg.Log.Dev = true
	}
	if cfg.Log.BufferSize <= 0 {
		cfg.Log.BufferSize = 512
	}
	if cfg.Log.File != "" {
		if cfg.Log.MaxSizeMB <= 0 {
			cfg.Log.MaxSizeMB = 100
		}
		if cfg.Log.MaxAgeDays <= 0 {
			cfg.Log.MaxAgeDays = 7
		}
	}
	if err := cfg.Auth.Normalize(); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

func (c Config) NewVerifier(ctx context.Context) (*auth.Verifier, error) {
	return auth.NewVerifier(ctx, c.Auth)
}
