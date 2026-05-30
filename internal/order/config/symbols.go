package config

import (
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

// SymbolRuleConfig 单交易对精度与最小下单约束（与 configs/symbols.json 字段一致）。
type SymbolRuleConfig = symbolrules.RuleConfig

func (c *Config) applySymbolDefaults() {
	if c.SymbolsFile != "" || len(c.Symbols) > 0 {
		return
	}
	c.SymbolsFile = "configs/symbols.json"
}

// SymbolRegistry 根据配置构建交易对规则表。
func (c Config) SymbolRegistry() (*symbolrules.Registry, error) {
	cfg, err := c.LoadRules()
	if err != nil {
		return nil, err
	}
	return cfg.Registry, nil
}

// LoadRules 加载交易对与资产精度。
func (c Config) LoadRules() (symbolrules.LoadedConfig, error) {
	cfg, err := symbolrules.LoadRulesFromConfig(c.SymbolsFile, c.Symbols)
	if err != nil {
		return symbolrules.LoadedConfig{}, fmt.Errorf("config: symbols: %w", err)
	}
	return cfg, nil
}
