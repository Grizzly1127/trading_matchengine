package symbolrules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/shopspring/decimal"
)

// Registry 交易对规则表。
type Registry struct {
	bySymbol map[string]Spec
}

// NewRegistry 从 Spec 列表构建。
func NewRegistry(specs ...Spec) (*Registry, error) {
	m := make(map[string]Spec, len(specs))
	for _, sp := range specs {
		key := normalizeKey(sp.Symbol)
		if key == "" {
			return nil, fmt.Errorf("symbolrules: empty symbol in spec")
		}
		if _, dup := m[key]; dup {
			return nil, fmt.Errorf("symbolrules: duplicate %q", sp.Symbol)
		}
		if sp.PricePrecision < 0 || sp.QuantityPrecision < 0 {
			return nil, fmt.Errorf("symbolrules: invalid precision for %q", sp.Symbol)
		}
		if sp.Status == "" {
			sp.Status = "TRADING"
		}
		m[key] = sp
	}
	return &Registry{bySymbol: m}, nil
}

// Lookup 按交易对查规则。
func (r *Registry) Lookup(symbol string) (Spec, error) {
	if r == nil {
		return Spec{}, fmt.Errorf("symbol registry not configured")
	}
	key := normalizeKey(symbol)
	if key == "" {
		return Spec{}, fmt.Errorf("symbol is required")
	}
	sp, ok := r.bySymbol[key]
	if !ok {
		return Spec{}, fmt.Errorf("unknown or unsupported symbol %q", symbol)
	}
	return sp, nil
}

// All 返回按 symbol 排序的全部规则（供 REST /v1/market/symbols）。
func (r *Registry) All() []Spec {
	if r == nil {
		return nil
	}
	out := make([]Spec, 0, len(r.bySymbol))
	for _, sp := range r.bySymbol {
		out = append(out, sp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

func normalizeKey(symbol string) string {
	return strings.TrimSpace(symbol)
}

// DefaultBTCUSDTSpec 开发默认交易对。
func DefaultBTCUSDTSpec() Spec {
	return Spec{
		Symbol:            "BTC-USDT",
		BaseAsset:         "BTC",
		QuoteAsset:        "USDT",
		PricePrecision:    2,
		QuantityPrecision: 6,
		MinQuantity:       decimal.RequireFromString("0.000001"),
		MinNotional:       decimal.RequireFromString("5"),
		Status:            "TRADING",
	}
}

// DefaultRegistry 单交易对 MVP 默认规则表。
func DefaultRegistry() (*Registry, error) {
	return NewRegistry(DefaultBTCUSDTSpec())
}

// RuleConfig JSON 配置项（内联或 symbols 文件）。
type RuleConfig struct {
	BaseAsset         string `json:"base_asset"`
	QuoteAsset        string `json:"quote_asset"`
	PricePrecision    int32  `json:"price_precision"`
	QuantityPrecision int32  `json:"quantity_precision"`
	MinQuantity       string `json:"min_quantity"`
	MinNotional       string `json:"min_notional"`
	Status            string `json:"status"`
}

// ParseSpec 从配置解析 Spec。
func ParseSpec(symbol string, rule RuleConfig) (Spec, error) {
	symbol = normalizeKey(symbol)
	if symbol == "" {
		return Spec{}, fmt.Errorf("symbol is required")
	}
	mq, err := decimal.NewFromString(strings.TrimSpace(rule.MinQuantity))
	if err != nil || !mq.IsPositive() {
		return Spec{}, fmt.Errorf("symbol %q: invalid min_quantity %q", symbol, rule.MinQuantity)
	}
	mn, err := decimal.NewFromString(strings.TrimSpace(rule.MinNotional))
	if err != nil || mn.IsNegative() {
		return Spec{}, fmt.Errorf("symbol %q: invalid min_notional %q", symbol, rule.MinNotional)
	}
	base := strings.TrimSpace(rule.BaseAsset)
	quote := strings.TrimSpace(rule.QuoteAsset)
	if base == "" || quote == "" {
		pair, err := ParsePair(symbol)
		if err != nil {
			return Spec{}, fmt.Errorf("symbol %q: %w", symbol, err)
		}
		base, quote = pair.Base, pair.Quote
	}
	status := strings.TrimSpace(rule.Status)
	if status == "" {
		status = "TRADING"
	}
	return Spec{
		Symbol:            symbol,
		BaseAsset:         base,
		QuoteAsset:        quote,
		PricePrecision:    rule.PricePrecision,
		QuantityPrecision: rule.QuantityPrecision,
		MinQuantity:       mq,
		MinNotional:       mn,
		Status:            status,
	}, nil
}

// RegistryFromMap 从 map[symbol]RuleConfig 构建。
func RegistryFromMap(m map[string]RuleConfig) (*Registry, error) {
	specs := make([]Spec, 0, len(m))
	for sym, rule := range m {
		sp, err := ParseSpec(sym, rule)
		if err != nil {
			return nil, err
		}
		specs = append(specs, sp)
	}
	return NewRegistry(specs...)
}
