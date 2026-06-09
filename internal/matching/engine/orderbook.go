package engine

import (
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/skiplist"
	"github.com/shopspring/decimal"
)

// OrderBook 是单个交易对的内存订单簿（卖盘 / 买盘各一棵跳表，值为 Order）。
type OrderBook struct {
	Symbol string

	asks     *skiplist.SkipList
	bids     *skiplist.SkipList
	orderMap map[uint64]Order
}

// NewOrderBook 创建空订单簿。
func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol:   symbol,
		asks:     skiplist.NewSkipList(compareAsk),
		bids:     skiplist.NewSkipList(compareBid),
		orderMap: make(map[uint64]Order, 2048),
	}
}

// compareAsk 卖盘升序：最低价在表头；同价 FIFO（UpdateTime），同价同时间再按 order_id 区分。
func compareAsk(a, b any) int {
	ao := a.(Order)
	bo := b.(Order)
	if cmp := ao.Price.Cmp(bo.Price); cmp != 0 {
		return cmp
	}
	if cmp := ao.UpdateTime.Compare(bo.UpdateTime); cmp != 0 {
		return cmp
	}
	if ao.OrderID != bo.OrderID {
		if ao.OrderID < bo.OrderID {
			return -1
		}
		return 1
	}
	return 0
}

// compareBid 买盘降序：最高价在表头；同价 FIFO（UpdateTime），同价同时间再按 order_id 区分。
func compareBid(a, b any) int {
	ao := a.(Order)
	bo := b.(Order)
	if cmp := bo.Price.Cmp(ao.Price); cmp != 0 {
		return cmp
	}
	if cmp := ao.UpdateTime.Compare(bo.UpdateTime); cmp != 0 {
		return cmp
	}
	if ao.OrderID != bo.OrderID {
		if ao.OrderID < bo.OrderID {
			return -1
		}
		return 1
	}
	return 0
}

func (b *OrderBook) getSiteBook(side Side) *skiplist.SkipList {
	if side == SideBuy {
		return b.bids
	}
	return b.asks
}

func (b *OrderBook) oppositeBook(takerSide Side) *skiplist.SkipList {
	if takerSide == SideBuy {
		return b.asks
	}
	return b.bids
}

// BestAsk 返回最低卖价；无卖盘时 ok=false。
func (b *OrderBook) BestAsk() (decimal.Decimal, bool) {
	v, ok := b.asks.Front()
	if !ok {
		return decimal.Zero, false
	}
	return v.(Order).Price, true
}

// BestBid 返回最高买价；无买盘时 ok=false。
func (b *OrderBook) BestBid() (decimal.Decimal, bool) {
	v, ok := b.bids.Front()
	if !ok {
		return decimal.Zero, false
	}
	return v.(Order).Price, true
}

func (b *OrderBook) InsertOrder(o Order) {
	if o.UpdateTime.IsZero() {
		o.UpdateTime = time.Now()
	}
	if existing, ok := b.orderMap[o.OrderID]; ok {
		if b.getSiteBook(existing.Side).Contains(existing) {
			return
		}
		delete(b.orderMap, o.OrderID)
	}
	b.orderMap[o.OrderID] = o
	b.getSiteBook(o.Side).Insert(o)
}

// RemoveOrder 从买卖两侧撤单（按 order_id）；不存在时幂等。
func (b *OrderBook) RemoveOrder(orderID uint64) {
	o, ok := b.orderMap[orderID]
	if !ok {
		return
	}
	b.getSiteBook(o.Side).Delete(o)
	delete(b.orderMap, orderID)
}

func (b *OrderBook) findInBook(orderID uint64) bool {
	o, ok := b.orderMap[orderID]
	if !ok {
		return false
	}
	return b.getSiteBook(o.Side).Contains(o)
}

func (b *OrderBook) FindOrder(orderID uint64) (Order, bool) {
	o, ok := b.orderMap[orderID]
	if !ok {
		return Order{}, false
	}
	return o, true
}

// HasActiveOrder 订单是否仍在盘口（对账用）。
func (b *OrderBook) HasActiveOrder(orderID uint64) bool {
	return b.findInBook(orderID)
}
