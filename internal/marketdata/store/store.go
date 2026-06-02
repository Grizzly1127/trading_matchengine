package store

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/shopspring/decimal"
)

// Store 保存 per-symbol 的行情聚合状态（内存）。
// 该 Store 同时被 trade 与 match 消费协程访问，因此内部做了细粒度锁。
type Store struct {
	mu      sync.RWMutex
	symbols map[string]*SymbolState
}

type SymbolState struct {
	mu sync.Mutex

	Ticker    TickerState
	OrderBook OrderBookState
	trades24h []tradeTick
}

type TickerState struct {
	LastPrice          decimal.Decimal
	OpenPrice          decimal.Decimal
	HighPrice          decimal.Decimal
	LowPrice           decimal.Decimal
	Volume             decimal.Decimal
	QuoteVolume        decimal.Decimal
	PriceChangePercent decimal.Decimal
	UpdatedAtMs        int64
}

type OrderBookState struct {
	LastUpdateID uint64
	Bids         OrderBookSide
	Asks         OrderBookSide
}

type OrderBookSide struct {
	// orderID -> entry
	Orders map[uint64]OrderEntry
	// price -> totalQty
	Levels map[string]decimal.Decimal
}

type OrderEntry struct {
	Side      string // "BUY" / "SELL"
	Price     decimal.Decimal
	Remaining decimal.Decimal
}

type PriceLevel struct {
	Price    string
	Quantity string
}

type OrderBookSnapshot struct {
	Symbol       string
	LastUpdateID uint64
	Bids         []PriceLevel
	Asks         []PriceLevel
	UpdatedAtMs  int64
}

type TickerSnapshot struct {
	Symbol string
	TickerState
}

type TickerAllSnapshot struct {
	QuoteAsset   string
	SnapshotID   string
	SnapshotTime int64
	Count        int
	Items        []TickerSnapshot
}

type ReferencePriceKind int

const (
	ReferencePriceBestAsk ReferencePriceKind = iota + 1
	ReferencePriceMark
	ReferencePriceLast
)

type ReferencePrice struct {
	Price       string
	UpdatedAtMs int64
	Kind        ReferencePriceKind
}

func New() *Store {
	return &Store{
		symbols: make(map[string]*SymbolState),
	}
}

func (s *Store) get(symbol string) (*SymbolState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := s.symbols[symbol]
	return st, st != nil
}

func (s *Store) Symbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	symbols := make([]string, 0, len(s.symbols))
	for symbol := range s.symbols {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)
	return symbols
}

func (s *Store) getOrCreate(symbol string) *SymbolState {
	s.mu.RLock()
	st := s.symbols[symbol]
	s.mu.RUnlock()
	if st != nil {
		return st
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if st = s.symbols[symbol]; st != nil {
		return st
	}
	st = &SymbolState{
		OrderBook: OrderBookState{
			Bids: OrderBookSide{Orders: make(map[uint64]OrderEntry), Levels: make(map[string]decimal.Decimal)},
			Asks: OrderBookSide{Orders: make(map[uint64]OrderEntry), Levels: make(map[string]decimal.Decimal)},
		},
	}
	s.symbols[symbol] = st
	return st
}

func parseDecimal(v string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(v)
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid decimal %q: %w", v, err)
	}
	return d, nil
}

// ApplyTrade 更新 Ticker（last_price、volume、quote_volume）。
func (s *Store) ApplyTrade(symbol string, priceStr string, qtyStr string, updatedAtMs int64) error {
	if symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	price, err := parseDecimal(priceStr)
	if err != nil {
		return err
	}
	qty, err := parseDecimal(qtyStr)
	if err != nil {
		return err
	}

	st := s.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()

	st.applyTrade24h(price, qty, updatedAtMs)
	return nil
}

// SnapshotTicker 读取当前 Ticker 快照（拷贝一份）。
func (s *Store) SnapshotTicker(symbol string) (TickerState, bool) {
	if symbol == "" {
		return TickerState{}, false
	}
	st, ok := s.get(symbol)
	if !ok {
		return TickerState{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.Ticker, true
}

// SnapshotTickerAll 返回按 symbol 排序的全市场 Ticker；quoteAsset 为空则返回全部。
func (s *Store) SnapshotTickerAll(quoteAsset string) []TickerSnapshot {
	s.mu.RLock()
	symbols := make([]string, 0, len(s.symbols))
	for symbol := range s.symbols {
		if quoteAsset == "" || symbolQuoteAsset(symbol) == quoteAsset {
			symbols = append(symbols, symbol)
		}
	}
	s.mu.RUnlock()

	sort.Strings(symbols)
	out := make([]TickerSnapshot, 0, len(symbols))
	for _, symbol := range symbols {
		t, ok := s.SnapshotTicker(symbol)
		if !ok || t.UpdatedAtMs == 0 {
			continue
		}
		out = append(out, TickerSnapshot{Symbol: symbol, TickerState: t})
	}
	return out
}

func (s *Store) BuildTickerAllSnapshot(quoteAsset string) TickerAllSnapshot {
	items := s.SnapshotTickerAll(quoteAsset)
	maxTs := int64(0)
	for _, it := range items {
		if it.UpdatedAtMs > maxTs {
			maxTs = it.UpdatedAtMs
		}
	}
	if maxTs == 0 {
		maxTs = 1
	}
	return TickerAllSnapshot{
		QuoteAsset:   quoteAsset,
		SnapshotID:   computeTickerAllSnapshotID(quoteAsset, items),
		SnapshotTime: maxTs,
		Count:        len(items),
		Items:        items,
	}
}

// SnapshotOrderBook 返回按买价降序、卖价升序排列的 topN 深度。
func (s *Store) SnapshotOrderBook(symbol string, limit int) (OrderBookSnapshot, bool) {
	if symbol == "" {
		return OrderBookSnapshot{}, false
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	st, ok := s.get(symbol)
	if !ok {
		return OrderBookSnapshot{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	return OrderBookSnapshot{
		Symbol:       symbol,
		LastUpdateID: st.OrderBook.LastUpdateID,
		Bids:         snapshotLevels(st.OrderBook.Bids.Levels, limit, true),
		Asks:         snapshotLevels(st.OrderBook.Asks.Levels, limit, false),
		UpdatedAtMs:  st.Ticker.UpdatedAtMs,
	}, true
}

func (s *Store) ReferencePrice(symbol string, kind ReferencePriceKind) (ReferencePrice, bool) {
	st, ok := s.get(symbol)
	if !ok {
		return ReferencePrice{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	switch kind {
	case ReferencePriceBestAsk:
		if price, ok := bestAsk(st.OrderBook.Asks.Levels); ok {
			return ReferencePrice{Price: price, UpdatedAtMs: st.Ticker.UpdatedAtMs, Kind: ReferencePriceBestAsk}, true
		}
		if !st.Ticker.LastPrice.IsZero() {
			return ReferencePrice{Price: st.Ticker.LastPrice.String(), UpdatedAtMs: st.Ticker.UpdatedAtMs, Kind: ReferencePriceLast}, true
		}
	case ReferencePriceMark, ReferencePriceLast:
		if !st.Ticker.LastPrice.IsZero() {
			return ReferencePrice{Price: st.Ticker.LastPrice.String(), UpdatedAtMs: st.Ticker.UpdatedAtMs, Kind: ReferencePriceLast}, true
		}
	}
	return ReferencePrice{}, false
}

// ApplyOrderBookAccepted 新增订单到镜像。
func (s *Store) ApplyOrderBookAccepted(symbol string, orderID uint64, side string, priceStr string, remainingStr string) error {
	if symbol == "" || orderID == 0 {
		return fmt.Errorf("symbol and order_id are required")
	}
	price, err := parseDecimal(priceStr)
	if err != nil {
		return err
	}
	rem, err := parseDecimal(remainingStr)
	if err != nil {
		return err
	}
	if rem.IsNegative() {
		return fmt.Errorf("remaining must be non-negative")
	}

	st := s.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()

	entry := OrderEntry{Side: side, Price: price, Remaining: rem}
	switch side {
	case "BUY":
		applyUpsert(&st.OrderBook.Bids, orderID, entry)
	case "SELL":
		applyUpsert(&st.OrderBook.Asks, orderID, entry)
	default:
		return fmt.Errorf("unknown side %q", side)
	}
	st.OrderBook.LastUpdateID++
	return nil
}

// ApplyOrderBookRemaining 更新订单 remaining（部分成交）。
func (s *Store) ApplyOrderBookRemaining(symbol string, orderID uint64, remainingStr string) error {
	if symbol == "" || orderID == 0 {
		return fmt.Errorf("symbol and order_id are required")
	}
	rem, err := parseDecimal(remainingStr)
	if err != nil {
		return err
	}
	if rem.IsNegative() {
		return fmt.Errorf("remaining must be non-negative")
	}

	st := s.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()

	if ok := applyRemaining(&st.OrderBook.Bids, orderID, rem); ok {
		st.OrderBook.LastUpdateID++
		return nil
	}
	if ok := applyRemaining(&st.OrderBook.Asks, orderID, rem); ok {
		st.OrderBook.LastUpdateID++
		return nil
	}
	// 可能是乱序或丢失 ACCEPTED；忽略。
	return nil
}

// ApplyOrderBookRemove 删除订单（filled/canceled）。
func (s *Store) ApplyOrderBookRemove(symbol string, orderID uint64) error {
	if symbol == "" || orderID == 0 {
		return fmt.Errorf("symbol and order_id are required")
	}
	st := s.getOrCreate(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()

	if ok := applyRemove(&st.OrderBook.Bids, orderID); ok {
		st.OrderBook.LastUpdateID++
		return nil
	}
	if ok := applyRemove(&st.OrderBook.Asks, orderID); ok {
		st.OrderBook.LastUpdateID++
	}
	return nil
}

func applyUpsert(side *OrderBookSide, orderID uint64, newEntry OrderEntry) {
	old, existed := side.Orders[orderID]
	if existed {
		// 先回滚旧 remaining 到价位汇总。
		priceKey := old.Price.String()
		side.Levels[priceKey] = side.Levels[priceKey].Sub(old.Remaining)
		if side.Levels[priceKey].IsZero() {
			delete(side.Levels, priceKey)
		}
	}

	side.Orders[orderID] = newEntry
	priceKey := newEntry.Price.String()
	side.Levels[priceKey] = side.Levels[priceKey].Add(newEntry.Remaining)
}

func applyRemaining(side *OrderBookSide, orderID uint64, newRemaining decimal.Decimal) bool {
	old, ok := side.Orders[orderID]
	if !ok {
		return false
	}
	if old.Remaining.Equal(newRemaining) {
		return true
	}
	priceKey := old.Price.String()
	delta := newRemaining.Sub(old.Remaining)
	side.Levels[priceKey] = side.Levels[priceKey].Add(delta)
	if side.Levels[priceKey].IsZero() {
		delete(side.Levels, priceKey)
	}
	old.Remaining = newRemaining
	side.Orders[orderID] = old
	return true
}

func applyRemove(side *OrderBookSide, orderID uint64) bool {
	old, ok := side.Orders[orderID]
	if !ok {
		return false
	}
	delete(side.Orders, orderID)
	priceKey := old.Price.String()
	side.Levels[priceKey] = side.Levels[priceKey].Sub(old.Remaining)
	if side.Levels[priceKey].IsZero() {
		delete(side.Levels, priceKey)
	}
	return true
}

func snapshotLevels(levels map[string]decimal.Decimal, limit int, desc bool) []PriceLevel {
	type level struct {
		price decimal.Decimal
		qty   decimal.Decimal
	}
	tmp := make([]level, 0, len(levels))
	for priceStr, qty := range levels {
		if !qty.IsPositive() {
			continue
		}
		price, err := decimal.NewFromString(priceStr)
		if err != nil {
			continue
		}
		tmp = append(tmp, level{price: price, qty: qty})
	}
	sort.Slice(tmp, func(i, j int) bool {
		cmp := tmp[i].price.Cmp(tmp[j].price)
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})
	if len(tmp) > limit {
		tmp = tmp[:limit]
	}
	out := make([]PriceLevel, 0, len(tmp))
	for _, lv := range tmp {
		out = append(out, PriceLevel{Price: lv.price.String(), Quantity: lv.qty.String()})
	}
	return out
}

func bestAsk(levels map[string]decimal.Decimal) (string, bool) {
	asks := snapshotLevels(levels, 1, false)
	if len(asks) == 0 {
		return "", false
	}
	return asks[0].Price, true
}

func symbolQuoteAsset(symbol string) string {
	idx := strings.LastIndex(symbol, "-")
	if idx < 0 || idx == len(symbol)-1 {
		return ""
	}
	return symbol[idx+1:]
}

func computeTickerAllSnapshotID(quoteAsset string, items []TickerSnapshot) string {
	var b strings.Builder
	b.WriteString(quoteAsset)
	b.WriteString("|")
	for _, it := range items {
		b.WriteString(it.Symbol)
		b.WriteString(":")
		b.WriteString(it.LastPrice.String())
		b.WriteString(":")
		b.WriteString(it.OpenPrice.String())
		b.WriteString(":")
		b.WriteString(it.HighPrice.String())
		b.WriteString(":")
		b.WriteString(it.LowPrice.String())
		b.WriteString(":")
		b.WriteString(it.Volume.String())
		b.WriteString(":")
		b.WriteString(it.QuoteVolume.String())
		b.WriteString(":")
		b.WriteString(it.PriceChangePercent.String())
		b.WriteString(":")
		b.WriteString(strconv.FormatInt(it.UpdatedAtMs, 10))
		b.WriteString("|")
	}
	sum := sha1.Sum([]byte(b.String()))
	return "snap-" + hex.EncodeToString(sum[:])
}
