package symbolrules

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Spec 交易对元数据与精度约束（Order / Gateway / Matching 共用）。
type Spec struct {
	Symbol            string
	BaseAsset         string
	QuoteAsset        string
	PricePrecision    int32
	QuantityPrecision int32
	MinQuantity       decimal.Decimal
	MinNotional       decimal.Decimal
	Status            string // TRADING | HALT
}

// ValidateQuantity 校验数量在 lot 网格上且 ≥ min_quantity，返回规范十进制字符串。
func (s Spec) ValidateQuantity(qty decimal.Decimal) (string, error) {
	if err := assertOnGrid(qty, s.QuantityPrecision, "quantity"); err != nil {
		return "", err
	}
	if s.MinQuantity.IsPositive() && qty.LessThan(s.MinQuantity) {
		return "", fmt.Errorf("quantity below min_quantity %s", s.MinQuantity.String())
	}
	return qty.String(), nil
}

// ValidatePrice 校验限价在 price tick 网格上，返回规范字符串。
func (s Spec) ValidatePrice(price decimal.Decimal) (string, error) {
	if err := assertOnGrid(price, s.PricePrecision, "price"); err != nil {
		return "", err
	}
	if !price.IsPositive() {
		return "", fmt.Errorf("price must be positive")
	}
	return price.String(), nil
}

// CeilPrice 将外部参考价向上取整到 tick（市价买冻结偏保守）。
func (s Spec) CeilPrice(price decimal.Decimal) (string, error) {
	if !price.IsPositive() {
		return "", fmt.Errorf("price must be positive")
	}
	ceiled := ceilToPlaces(price, s.PricePrecision)
	return ceiled.String(), nil
}

// CheckMinNotional 校验 price×qty 名义价值。
func (s Spec) CheckMinNotional(price, qty decimal.Decimal) error {
	if s.MinNotional.IsZero() {
		return nil
	}
	if price.Mul(qty).LessThan(s.MinNotional) {
		return fmt.Errorf("notional below min_notional %s", s.MinNotional.String())
	}
	return nil
}

func assertOnGrid(v decimal.Decimal, places int32, field string) error {
	if places < 0 {
		return fmt.Errorf("invalid %s precision", field)
	}
	truncated := v.Truncate(places)
	if !v.Equal(truncated) {
		return fmt.Errorf("%s %s exceeds allowed precision (max %d decimal places)", field, v.String(), places)
	}
	return nil
}

func ceilToPlaces(v decimal.Decimal, places int32) decimal.Decimal {
	if places < 0 {
		return v
	}
	truncated := v.Truncate(places)
	if v.Equal(truncated) {
		return truncated
	}
	step := decimal.New(1, -places)
	return truncated.Add(step)
}
