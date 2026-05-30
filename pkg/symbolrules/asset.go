package symbolrules

import (
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// AssetRuleConfig 单资产小数位配置。
type AssetRuleConfig struct {
	Precision int32 `json:"precision"`
}

// AssetRegistry 资产精度表（余额入账/出账舍入）。
type AssetRegistry struct {
	byAsset map[string]int32
	defaultPrecision int32
}

// NewAssetRegistry 构建资产精度表；未列出的资产使用 defaultPrecision（默认 8）。
func NewAssetRegistry(rules map[string]AssetRuleConfig, defaultPrecision int32) (*AssetRegistry, error) {
	if defaultPrecision < 0 {
		return nil, fmt.Errorf("symbolrules: invalid default asset precision")
	}
	if defaultPrecision == 0 {
		defaultPrecision = 8
	}
	m := make(map[string]int32, len(rules))
	for asset, rule := range rules {
		key := normalizeAsset(asset)
		if key == "" {
			return nil, fmt.Errorf("symbolrules: empty asset name")
		}
		if rule.Precision < 0 {
			return nil, fmt.Errorf("symbolrules: invalid precision for asset %q", asset)
		}
		p := rule.Precision
		if p == 0 {
			p = defaultPrecision
		}
		m[key] = p
	}
	return &AssetRegistry{byAsset: m, defaultPrecision: defaultPrecision}, nil
}

// DefaultAssetRegistry 开发环境默认（BTC/USDT 8 位）。
func DefaultAssetRegistry() (*AssetRegistry, error) {
	return NewAssetRegistry(map[string]AssetRuleConfig{
		"BTC":  {Precision: 8},
		"USDT": {Precision: 8},
	}, 8)
}

// Ensure 注册或补齐资产精度。
func (a *AssetRegistry) Ensure(asset string, precision int32) {
	if a == nil {
		return
	}
	key := normalizeAsset(asset)
	if key == "" || precision < 0 {
		return
	}
	if _, ok := a.byAsset[key]; !ok {
		if a.byAsset == nil {
			a.byAsset = make(map[string]int32)
		}
		p := precision
		if p == 0 {
			p = a.defaultPrecision
		}
		a.byAsset[key] = p
	}
}

// Precision 返回资产小数位。
func (a *AssetRegistry) Precision(asset string) int32 {
	if a == nil {
		return 8
	}
	key := normalizeAsset(asset)
	if p, ok := a.byAsset[key]; ok {
		return p
	}
	if a.defaultPrecision > 0 {
		return a.defaultPrecision
	}
	return 8
}

// RoundDown 结算扣款/入账：向下取整到资产精度（避免透支冻结）。
func (a *AssetRegistry) RoundDown(asset string, v decimal.Decimal) decimal.Decimal {
	if v.IsNegative() {
		return v.Truncate(a.Precision(asset))
	}
	return v.Truncate(a.Precision(asset))
}

// RoundUp 冻结：向上取整到资产精度（偏保守）。
func (a *AssetRegistry) RoundUp(asset string, v decimal.Decimal) decimal.Decimal {
	if v.IsNegative() {
		return v
	}
	p := a.Precision(asset)
	truncated := v.Truncate(p)
	if v.Equal(truncated) {
		return truncated
	}
	step := decimal.New(1, -p)
	return truncated.Add(step)
}

func normalizeAsset(asset string) string {
	return strings.TrimSpace(strings.ToUpper(asset))
}

// EnrichAssetsFromSymbols 用交易对 base/quote 补齐未配置的资产（默认精度）。
func EnrichAssetsFromSymbols(assets *AssetRegistry, reg *Registry, defaultPrecision int32) {
	if assets == nil || reg == nil {
		return
	}
	if defaultPrecision <= 0 {
		defaultPrecision = 8
	}
	for _, sp := range reg.All() {
		assets.Ensure(sp.BaseAsset, defaultPrecision)
		assets.Ensure(sp.QuoteAsset, defaultPrecision)
	}
}
