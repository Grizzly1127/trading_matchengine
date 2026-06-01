package symbol

import (
	"fmt"
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
	if s.IsReadOnly(taker.Symbol) {
		return nil, engine.ErrSymbolReadOnly
	}
	se, ok := s.Get(taker.Symbol)
	if !ok {
		return nil, fmt.Errorf("unknown symbol %q", taker.Symbol)
	}
	return se.Match(taker, commandSeq)
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

// SetReadOnly 将交易对设为只读拒单。
func (s *Shard) SetReadOnly(symbol, reason string) {
	if symbol == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	se := s.symbols[symbol]
	if se == nil {
		se = NewSymbolEngine(symbol, "", "")
		s.symbols[symbol] = se
	}
	se.ReadOnly = true
	se.ReadOnlyReason = reason
}

// IsReadOnly 是否处于只读拒单状态。
func (s *Shard) IsReadOnly(symbol string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	se, ok := s.symbols[symbol]
	return ok && se != nil && se.ReadOnly
}

// ReadOnlyReason 返回只读原因；未只读时为空。
func (s *Shard) ReadOnlyReason(symbol string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	se, ok := s.symbols[symbol]
	if !ok || se == nil || !se.ReadOnly {
		return ""
	}
	return se.ReadOnlyReason
}
