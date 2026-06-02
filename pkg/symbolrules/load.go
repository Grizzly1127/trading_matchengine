package symbolrules

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// File 共享 symbols 配置文件（configs/symbols.json）。
type File struct {
	Symbols map[string]RuleConfig      `json:"symbols"`
	Assets  map[string]AssetRuleConfig `json:"assets"`
}

// LoadedConfig 从 symbols 配置文件解析出的规则表。
type LoadedConfig struct {
	Registry *Registry
	Assets   *AssetRegistry
}

// LoadFromFile 加载交易对与资产精度配置。
func LoadFromFile(path string) (LoadedConfig, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return LoadedConfig{}, fmt.Errorf("symbolrules: path is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return LoadedConfig{}, fmt.Errorf("symbolrules: read %q: %w", path, err)
	}
	return ParseFile(b)
}

// ParseFile 解析 JSON 配置字节。
func ParseFile(b []byte) (LoadedConfig, error) {
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return LoadedConfig{}, fmt.Errorf("symbolrules: parse: %w", err)
	}

	var reg *Registry
	var err error
	if len(f.Symbols) == 0 {
		reg, err = DefaultRegistry()
	} else {
		reg, err = RegistryFromMap(f.Symbols)
	}
	if err != nil {
		return LoadedConfig{}, err
	}

	assets, err := NewAssetRegistry(f.Assets, 8)
	if err != nil {
		return LoadedConfig{}, err
	}
	EnrichAssetsFromSymbols(assets, reg, 8)

	return LoadedConfig{Registry: reg, Assets: assets}, nil
}

// LoadFile 兼容旧接口：仅返回交易对 Registry。
func LoadFile(path string) (*Registry, error) {
	cfg, err := LoadFromFile(path)
	if err != nil {
		return nil, err
	}
	return cfg.Registry, nil
}
