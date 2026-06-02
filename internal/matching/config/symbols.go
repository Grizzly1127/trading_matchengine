package config

import (
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
)

type SymbolRuleConfig = symbolrules.RuleConfig

func (c *Config) applySymbolDefaults() {
	if c.SymbolsFile != "" || len(c.Symbols) > 0 {
		return
	}
	c.SymbolsFile = "configs/symbols.json"
}

func (c Config) SymbolRegistry() (*symbolrules.Registry, error) {
	reg, err := symbolrules.LoadRegistryFromConfig(c.SymbolsFile, c.Symbols)
	if err != nil {
		return nil, fmt.Errorf("config: symbols: %w", err)
	}
	return reg, nil
}
