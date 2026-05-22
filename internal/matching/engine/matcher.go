package engine

import (
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/skiplist"
	"github.com/shopspring/decimal"
)

// Match 在单个订单簿上撮合：先吃对手盘；限价单剩余挂本侧，市价单剩余丢弃。
func (b *OrderBook) Match(taker Order, commandSeq uint64) ([]Trade, error) {
	taker = prepareTaker(taker)

	switch taker.Type {
	case OrderTypeLimit:
		return b.matchLimit(taker, commandSeq)
	case OrderTypeMarket:
		return b.matchMarket(taker, commandSeq)
	default:
		return nil, fmt.Errorf("unsupported order type: %v", taker.Type)
	}
}

func prepareTaker(o Order) Order {
	if o.Remaining.IsZero() {
		o.Remaining = o.Quantity
	}
	return o
}

func (b *OrderBook) matchLimit(taker Order, commandSeq uint64) ([]Trade, error) {
	opposite := b.oppositeBook(taker.Side)
	trades, taker, err := matchAgainstBook(b, opposite, taker, commandSeq)
	if err != nil {
		return trades, err
	}

	if taker.Remaining.IsPositive() {
		b.InsertOrder(taker)
	}
	return trades, nil
}

func (b *OrderBook) matchMarket(taker Order, commandSeq uint64) ([]Trade, error) {
	opposite := b.oppositeBook(taker.Side)
	trades, _, err := matchAgainstBook(b, opposite, taker, commandSeq)
	return trades, err
}

func matchAgainstBook(b *OrderBook, book *skiplist.SkipList, taker Order, commandSeq uint64) ([]Trade, Order, error) {
	trades := make([]Trade, 0)
	iter := book.Iterator()
	for value, ok := iter.Next(); ok; value, ok = iter.Next() {
		if taker.Remaining.IsZero() {
			break
		}

		maker := value.(Order)
		if stopMatch(taker, maker.Price) {
			break
		}

		matchQty := decimal.Min(taker.Remaining, maker.Remaining)

		maker.Remaining = maker.Remaining.Sub(matchQty)
		if maker.Remaining.IsZero() {
			book.Delete(maker)
			delete(b.orderMap, maker.OrderID)
		} else {
			book.Delete(maker)
			book.Insert(maker)
			b.orderMap[maker.OrderID] = maker
		}
		maker.UpdateTime = time.Now()

		taker.Remaining = taker.Remaining.Sub(matchQty)
		taker.UpdateTime = time.Now()

		trades = append(trades, Trade{
			TradeID:      DeriveTradeID(commandSeq, maker.OrderID, taker.OrderID),
			Symbol:       taker.Symbol,
			CreateTime:   taker.UpdateTime,
			Price:        maker.Price,
			Quantity:     matchQty,
			MakerOrderID: maker.OrderID,
			TakerOrderID: taker.OrderID,
		})
	}

	return trades, taker, nil
}

func stopMatch(taker Order, makerPrice decimal.Decimal) bool {
	if taker.Type == OrderTypeMarket {
		return false
	}
	if taker.Side == SideBuy {
		return makerPrice.GreaterThan(taker.Price)
	}
	return makerPrice.LessThan(taker.Price)
}
