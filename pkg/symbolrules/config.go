package symbolrules

import "fmt"

// LoadRegistryFromConfig 优先 symbols 文件，其次内联 map，最后默认 BTC-USDT。
func LoadRegistryFromConfig(symbolsFile string, inline map[string]RuleConfig) (*Registry, error) {
	cfg, err := LoadRulesFromConfig(symbolsFile, inline)
	if err != nil {
		return nil, err
	}
	return cfg.Registry, nil
}

// LoadRulesFromConfig 加载交易对 + 资产精度。
func LoadRulesFromConfig(symbolsFile string, inline map[string]RuleConfig) (LoadedConfig, error) {
	if symbolsFile != "" {
		return LoadFromFile(symbolsFile)
	}
	if len(inline) > 0 {
		reg, err := RegistryFromMap(inline)
		if err != nil {
			return LoadedConfig{}, err
		}
		assets, err := DefaultAssetRegistry()
		if err != nil {
			return LoadedConfig{}, err
		}
		EnrichAssetsFromSymbols(assets, reg, 8)
		return LoadedConfig{Registry: reg, Assets: assets}, nil
	}
	reg, err := DefaultRegistry()
	if err != nil {
		return LoadedConfig{}, err
	}
	assets, err := DefaultAssetRegistry()
	if err != nil {
		return LoadedConfig{}, err
	}
	return LoadedConfig{Registry: reg, Assets: assets}, nil
}

// MustLoadRegistryFromConfig 同 LoadRegistryFromConfig，失败 panic（仅测试）。
func MustLoadRegistryFromConfig(symbolsFile string, inline map[string]RuleConfig) *Registry {
	r, err := LoadRegistryFromConfig(symbolsFile, inline)
	if err != nil {
		panic(fmt.Sprintf("symbolrules: %v", err))
	}
	return r
}
