package symbol

import (
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
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
}

// NewSymbolEngine 创建交易对引擎。
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

// Match 在本交易对订单簿上撮合。
func (se *SymbolEngine) Match(taker engine.Order, commandSeq uint64) ([]engine.Trade, error) {
	if taker.Symbol == "" {
		taker.Symbol = se.Symbol
	}
	if taker.Symbol != se.Symbol {
		return nil, fmt.Errorf("symbol mismatch: got %s want %s", taker.Symbol, se.Symbol)
	}
	return se.OrderBook.Match(taker, commandSeq)
}

// Cancel 撤销挂单；不存在时幂等。
func (se *SymbolEngine) Cancel(orderID uint64) {
	se.OrderBook.RemoveOrder(orderID)
}
