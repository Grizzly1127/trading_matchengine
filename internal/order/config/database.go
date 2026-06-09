package config

import "encoding/json"

// DatabaseConfig PostgreSQL 连接池；API 与 Relay 分池避免争用。
type DatabaseConfig struct {
	MaxConns      int `json:"max_conns"`
	RelayMaxConns int `json:"relay_max_conns"`
}

func (c *Config) applyDatabaseDefaults(raw map[string]json.RawMessage) {
	if _, ok := raw["database"]; !ok {
		if c.Database.MaxConns <= 0 {
			c.Database.MaxConns = 50
		}
		if c.Database.RelayMaxConns <= 0 {
			c.Database.RelayMaxConns = 20
		}
		return
	}
	if c.Database.MaxConns <= 0 {
		c.Database.MaxConns = 50
	}
	if c.Database.RelayMaxConns <= 0 {
		c.Database.RelayMaxConns = 20
	}
}
