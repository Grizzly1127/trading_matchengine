package repository

import (
	"fmt"

	"github.com/shopspring/decimal"

	ordersymbol "github.com/Grizzly1127/trading_matchengine/internal/order/symbol"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

// FreezeSpec 下单时需冻结的资产与数量。
type FreezeSpec struct {
	Asset  string
	Amount decimal.Decimal
}

// ComputeFreeze 计算下单冻结（限价买卖、市价卖）。
// 市价买：当前 Phase 1 要求 price 作临时保护价；目标方案见 docs/design/market-buy-freeze.md（方案 C，依赖 Market Data）。
func ComputeFreeze(side int16, symbolName string, price *string, quantity string) (FreezeSpec, error) {
	pair, err := ordersymbol.ParsePair(symbolName)
	if err != nil {
		return FreezeSpec{}, err
	}
	qty, err := decimal.NewFromString(quantity)
	if err != nil || !qty.IsPositive() {
		return FreezeSpec{}, fmt.Errorf("invalid quantity")
	}

	switch commonv1.Side(side) {
	case commonv1.Side_SIDE_BUY:
		if price == nil || *price == "" {
			return FreezeSpec{}, fmt.Errorf("price required to freeze quote for buy order")
		}
		p, err := decimal.NewFromString(*price)
		if err != nil || !p.IsPositive() {
			return FreezeSpec{}, fmt.Errorf("invalid price")
		}
		return FreezeSpec{Asset: pair.Quote, Amount: p.Mul(qty)}, nil
	case commonv1.Side_SIDE_SELL:
		return FreezeSpec{Asset: pair.Base, Amount: qty}, nil
	default:
		return FreezeSpec{}, fmt.Errorf("invalid side")
	}
}

// RemainingFreeze 计算撤单/终态后应释放的剩余冻结。
func RemainingFreeze(o *Order) (FreezeSpec, error) {
	pair, err := ordersymbol.ParsePair(o.Symbol)
	if err != nil {
		return FreezeSpec{}, err
	}
	qty, err := decimal.NewFromString(o.Quantity)
	if err != nil {
		return FreezeSpec{}, err
	}
	filled, err := decimal.NewFromString(o.FilledQuantity)
	if err != nil {
		return FreezeSpec{}, err
	}
	remaining := qty.Sub(filled)
	if remaining.IsNegative() {
		remaining = decimal.Zero
	}
	if remaining.IsZero() {
		return FreezeSpec{}, nil
	}

	switch commonv1.Side(o.Side) {
	case commonv1.Side_SIDE_SELL:
		return FreezeSpec{Asset: pair.Base, Amount: remaining}, nil
	case commonv1.Side_SIDE_BUY:
		if o.Price == nil || *o.Price == "" {
			return FreezeSpec{}, fmt.Errorf("order price required to release buy freeze")
		}
		p, err := decimal.NewFromString(*o.Price)
		if err != nil {
			return FreezeSpec{}, err
		}
		return FreezeSpec{Asset: pair.Quote, Amount: p.Mul(remaining)}, nil
	default:
		return FreezeSpec{}, fmt.Errorf("invalid side")
	}
}
