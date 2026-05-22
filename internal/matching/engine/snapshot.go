package engine

import (
	"errors"
	"fmt"
	"time"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/shopspring/decimal"
)

// ErrSpreadViolation 表示买卖盘口价格倒挂。
var ErrSpreadViolation = errors.New("orderbook spread violation")

// ExportSnapshot 将当前订单簿导出为 protobuf 快照。
func (b *OrderBook) ExportSnapshot(shardID string, seqID uint64, ts time.Time) *matchingv1.Snapshot {
	if ts.IsZero() {
		ts = time.Now()
	}

	orderMap := make(map[uint64]*commonv1.Order)
	for _, o := range b.ActiveOrders() {
		orderMap[o.OrderID] = OrderToProto(o)
	}

	return &matchingv1.Snapshot{
		ShardId:           shardID,
		Symbol:            b.Symbol,
		SeqId:             seqID,
		TimestampUnixNano: ts.UnixNano(),
		Bids:              b.exportLevels(SideBuy),
		Asks:              b.exportLevels(SideSell),
		OrderMap:          orderMap,
	}
}

// RestoreFromSnapshot 从 protobuf 快照恢复订单簿。
func (b *OrderBook) RestoreFromSnapshot(snap *matchingv1.Snapshot) error {
	if snap == nil {
		return fmt.Errorf("snapshot is nil")
	}

	*b = *NewOrderBook(snap.GetSymbol())
	if err := b.restoreLevels(snap.GetBids()); err != nil {
		return err
	}
	if err := b.restoreLevels(snap.GetAsks()); err != nil {
		return err
	}
	return b.Validate()
}

// Validate 校验盘口合法性：无倒挂且 order_map 与挂单数量一致（若提供 map）。
func (b *OrderBook) Validate() error {
	bid, hasBid := b.BestBid()
	ask, hasAsk := b.BestAsk()
	if hasBid && hasAsk && !bid.LessThan(ask) {
		return ErrSpreadViolation
	}
	return nil
}

// ValidateWithOrderMap 校验盘口及 order_map 条目数与挂单总数一致。
func (b *OrderBook) ValidateWithOrderMap(orderMap map[uint64]*commonv1.Order) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if orderMap == nil {
		return nil
	}
	active := b.ActiveOrderCount()
	if len(orderMap) != active {
		return fmt.Errorf("order_map size %d != active orders %d", len(orderMap), active)
	}
	for id := range orderMap {
		if !b.findInBook(id) {
			return fmt.Errorf("order_map contains inactive order_id %d", id)
		}
	}
	return nil
}

// ActiveOrderCount 返回买卖两侧挂单总数。
func (b *OrderBook) ActiveOrderCount() int {
	return b.bids.Size() + b.asks.Size()
}

// ActiveOrders 返回所有活跃订单（order_id → Order）。
func (b *OrderBook) ActiveOrders() map[uint64]Order {
	out := make(map[uint64]Order)
	for _, o := range b.orderMap {
		out[o.OrderID] = o
	}
	return out
}

func (b *OrderBook) exportLevels(side Side) []*matchingv1.PriceLevel {
	book := b.getSiteBook(side)
	levels := make([]*matchingv1.PriceLevel, 0)
	var current *matchingv1.PriceLevel
	var lastPrice decimal.Decimal
	hasLast := false

	it := book.Iterator()
	for value, ok := it.Next(); ok; value, ok = it.Next() {
		o := value.(Order)
		if !hasLast || !o.Price.Equal(lastPrice) {
			if current != nil {
				levels = append(levels, current)
			}
			current = &matchingv1.PriceLevel{Price: decimalToProto(o.Price)}
			lastPrice = o.Price
			hasLast = true
		}
		current.Orders = append(current.Orders, OrderToProto(o))
	}
	if current != nil {
		levels = append(levels, current)
	}
	return levels
}

func (b *OrderBook) restoreLevels(levels []*matchingv1.PriceLevel) error {
	for _, level := range levels {
		for _, pb := range level.GetOrders() {
			o, err := OrderFromProto(pb)
			if err != nil {
				return err
			}
			if o.Symbol == "" {
				o.Symbol = b.Symbol
			}
			b.InsertOrder(o)
		}
	}
	return nil
}
