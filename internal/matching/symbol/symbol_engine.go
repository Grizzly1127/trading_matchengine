package symbol

import (
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	"github.com/shopspring/decimal"
)

// SymbolEngine 单交易对撮合运行时：配置 + 订单簿。
type SymbolEngine struct {
	Symbol            string
	BaseAsset         string
	QuoteAsset        string
	PricePrecision    int32
	QuantityPrecision int32
	MinQuantity       decimal.Decimal
	OrderBook         *engine.OrderBook
	// ReadOnly 为 true 时拒收新单（§5.6 对账不一致）。
	ReadOnly       bool
	ReadOnlyReason string
}

// NewSymbolEngine 创建空规则交易对引擎（测试用）。
func NewSymbolEngine(symbol, baseAsset, quoteAsset string) *SymbolEngine {
	return &SymbolEngine{
		Symbol:            symbol,
		BaseAsset:         baseAsset,
		QuoteAsset:        quoteAsset,
		PricePrecision:    0,
		QuantityPrecision: 0,
		OrderBook:         engine.NewOrderBook(symbol),
	}
}

// NewSymbolEngineFromSpec 从共享规则创建引擎。
func NewSymbolEngineFromSpec(sp symbolrules.Spec) *SymbolEngine {
	return &SymbolEngine{
		Symbol:            sp.Symbol,
		BaseAsset:         sp.BaseAsset,
		QuoteAsset:        sp.QuoteAsset,
		PricePrecision:    sp.PricePrecision,
		QuantityPrecision: sp.QuantityPrecision,
		MinQuantity:       sp.MinQuantity,
		OrderBook:         engine.NewOrderBook(sp.Symbol),
	}
}

func (se *SymbolEngine) rulesSpec() symbolrules.Spec {
	return symbolrules.Spec{
		Symbol:            se.Symbol,
		BaseAsset:         se.BaseAsset,
		QuoteAsset:        se.QuoteAsset,
		PricePrecision:    se.PricePrecision,
		QuantityPrecision: se.QuantityPrecision,
		MinQuantity:       se.MinQuantity,
	}
}

func (se *SymbolEngine) hasRules() bool {
	return se.PricePrecision > 0 || se.QuantityPrecision > 0 || se.MinQuantity.IsPositive()
}

// ValidateOrder 防御性校验（Order 入单应已规范化；未配置规则时跳过）。
func (se *SymbolEngine) ValidateOrder(o engine.Order) error {
	if !se.hasRules() {
		return nil
	}
	rs := se.rulesSpec()
	if _, err := rs.ValidateQuantity(o.Quantity); err != nil {
		return err
	}
	if o.Type == engine.OrderTypeLimit && o.Price.IsPositive() {
		if _, err := rs.ValidatePrice(o.Price); err != nil {
			return err
		}
	}
	return nil
}

// Match 在本交易对订单簿上撮合。
func (se *SymbolEngine) Match(taker engine.Order, commandSeq uint64) ([]engine.Trade, error) {
	if taker.Symbol == "" {
		taker.Symbol = se.Symbol
	}
	if taker.Symbol != se.Symbol {
		return nil, fmt.Errorf("symbol mismatch: got %s want %s", taker.Symbol, se.Symbol)
	}
	if err := se.ValidateOrder(taker); err != nil {
		return nil, fmt.Errorf("order validation: %w", err)
	}
	return se.OrderBook.Match(taker, commandSeq)
}

// Cancel 撤销挂单；不存在时幂等。
func (se *SymbolEngine) Cancel(orderID uint64) {
	se.OrderBook.RemoveOrder(orderID)
}
