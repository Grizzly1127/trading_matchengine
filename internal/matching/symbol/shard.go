package symbol

import (
	"sync"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
)

// Shard 撮合分片：按 symbol 路由到 SymbolEngine（对应架构文档中的多交易对引擎）。
type Shard struct {
	mu      sync.RWMutex
	symbols map[string]*SymbolEngine
}

// NewShard 创建空分片。
func NewShard() *Shard {
	return &Shard{symbols: make(map[string]*SymbolEngine)}
}

// Register 注册或替换交易对引擎（上线交易对、加载配置后调用）。
func (s *Shard) Register(se *SymbolEngine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.symbols[se.Symbol] = se
}

// Get 返回已注册的交易对引擎。
func (s *Shard) Get(symbol string) (*SymbolEngine, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	se, ok := s.symbols[symbol]
	return se, ok
}

// Symbol 返回交易对引擎；未注册时用 symbol 名创建默认引擎（便于测试）。
func (s *Shard) Symbol(symbol string) *SymbolEngine {
	s.mu.Lock()
	defer s.mu.Unlock()
	if se, ok := s.symbols[symbol]; ok {
		return se
	}
	se := NewSymbolEngine(symbol, "", "")
	s.symbols[symbol] = se
	return se
}

// Match 按 symbol 路由并撮合。
func (s *Shard) Match(taker engine.Order, commandSeq uint64) ([]engine.Trade, error) {
	if taker.Symbol == "" {
		return nil, engine.ErrSymbolRequired
	}
	return s.Symbol(taker.Symbol).Match(taker, commandSeq)
}

// Cancel 按 symbol 撤单。
func (s *Shard) Cancel(symbol string, orderID uint64) error {
	if symbol == "" {
		return engine.ErrSymbolRequired
	}
	s.Symbol(symbol).Cancel(orderID)
	return nil
}

// Symbols 返回已注册的交易对名称列表。
func (s *Shard) Symbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.symbols))
	for sym := range s.symbols {
		out = append(out, sym)
	}
	return out
}
