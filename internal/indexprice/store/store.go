package store

import (
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// Snapshot 内存中的指数价快照。
type Snapshot struct {
	Symbol  string
	Price   decimal.Decimal
	Sources []string
	Updated time.Time
	Stale   bool
}

// Store 各 symbol 最新指数价（供 gRPC 读取）。
type Store struct {
	mu   sync.RWMutex
	data map[string]Snapshot
}

func New() *Store {
	return &Store{data: make(map[string]Snapshot)}
}

// Update 写入最新有效价格。
func (s *Store) Update(symbol string, price decimal.Decimal, sources []string, at time.Time, stale bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]string(nil), sources...)
	s.data[symbol] = Snapshot{
		Symbol:  symbol,
		Price:   price,
		Sources: cp,
		Updated: at,
		Stale:   stale,
	}
}

// Get 读取 symbol 快照。
func (s *Store) Get(symbol string) (Snapshot, bool) {
	if s == nil {
		return Snapshot{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[symbol]
	return v, ok
}
