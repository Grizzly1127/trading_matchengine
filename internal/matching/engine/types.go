package engine

import (
	"time"

	"github.com/shopspring/decimal"
)

// Side is order direction.
type Side int

const (
	SideBuy  Side = 1
	SideSell Side = 2
)

type OrderType int

const (
	OrderTypeLimit  OrderType = 1
	OrderTypeMarket OrderType = 2
)

// Order is a limit or market order in the matching engine.
type Order struct {
	OrderID       uint64
	ClientOrderID string
	Symbol        string
	CreateTime    time.Time
	UpdateTime    time.Time
	Side          Side
	Type          OrderType
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	Remaining     decimal.Decimal
	Flags         uint64
}

// Trade is a fill produced by the matcher.
type Trade struct {
	TradeID      uint64
	Symbol       string
	CreateTime   time.Time
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	MakerOrderID uint64
	TakerOrderID uint64
}
