package repository

import (
	"fmt"

	"github.com/shopspring/decimal"

	ordersymbol "github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

// FreezeSpec 下单时需冻结的资产与数量。
type FreezeSpec struct {
	Asset  string
	Amount decimal.Decimal
}

// ComputeFreeze 计算下单冻结（限价买卖、市价卖、市价买）。
// 若 frozenAmount 非空，买单优先按该冻结值（市价买方案 C）。
func ComputeFreeze(side int16, symbolName string, price *string, quantity string, frozenAmount *string) (FreezeSpec, error) {
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
		if frozenAmount != nil && *frozenAmount != "" {
			a, err := decimal.NewFromString(*frozenAmount)
			if err != nil || !a.IsPositive() {
				return FreezeSpec{}, fmt.Errorf("invalid frozen_amount")
			}
			return FreezeSpec{Asset: pair.Quote, Amount: a}, nil
		}
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
		if o.FrozenAmount != nil && *o.FrozenAmount != "" {
			total, err := decimal.NewFromString(*o.FrozenAmount)
			if err != nil {
				return FreezeSpec{}, err
			}
			if qty.IsZero() {
				return FreezeSpec{}, nil
			}
			remainAmt := total.Mul(remaining).Div(qty)
			return FreezeSpec{Asset: pair.Quote, Amount: remainAmt}, nil
		}
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
