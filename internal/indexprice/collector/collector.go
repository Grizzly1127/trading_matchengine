package collector

import (
	"context"

	"github.com/shopspring/decimal"
)

// Collector 外部价格源。
type Collector interface {
	Name() string
	Weight() decimal.Decimal
	FetchPrice(ctx context.Context, symbol string) (decimal.Decimal, error)
}
