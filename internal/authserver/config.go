package authserver

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config cmd/auth 配置。
type Config struct {
	HTTPListen      string         `json:"http_listen"`
	Issuer          string         `json:"issuer"`
	Audience        []string       `json:"audience"`
	HS256SecretFile string         `json:"hs256_secret_file"`
	TokenTTLSeconds int            `json:"token_ttl_seconds"`
	Clients         []ClientConfig `json:"clients"`
	Log             logConfig      `json:"log"`
}

type ClientConfig struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	Scopes       []string `json:"scopes"`
}

type logConfig struct {
	Level string `json:"level"`
	Dev   bool   `json:"dev"`
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("auth config: read %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("auth config: parse: %w", err)
	}
	if cfg.HTTPListen == "" {
		cfg.HTTPListen = ":8090"
	}
	if cfg.Issuer == "" {
		cfg.Issuer = "trading-matchengine-dev"
	}
	if len(cfg.Audience) == 0 {
		cfg.Audience = []string{"trading-gateway", "trading-push"}
	}
	if cfg.TokenTTLSeconds <= 0 {
		cfg.TokenTTLSeconds = 900
	}
	if strings.TrimSpace(cfg.HS256SecretFile) == "" {
		return Config{}, fmt.Errorf("auth config: hs256_secret_file is required")
	}
	if len(cfg.Clients) == 0 {
		return Config{}, fmt.Errorf("auth config: at least one client")
	}
	for i, c := range cfg.Clients {
		if strings.TrimSpace(c.ClientID) == "" || strings.TrimSpace(c.ClientSecret) == "" {
			return Config{}, fmt.Errorf("auth config: client[%d] id/secret required", i)
		}
		if len(c.Scopes) == 0 {
			return Config{}, fmt.Errorf("auth config: client[%d] scopes required", i)
		}
	}
	return cfg, nil
}
