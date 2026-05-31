package collector

import (
	"context"
	"hash/fnv"
	"sync"

	"github.com/shopspring/decimal"
)

// Mock 本地模拟价格源（离线联调）。
type Mock struct {
	weight      decimal.Decimal
	basePrices  map[string]decimal.Decimal
	defaultBase decimal.Decimal
	mu          sync.Mutex
	tick        uint64
}

// MockConfig Mock 配置。
type MockConfig struct {
	Weight      decimal.Decimal
	BasePrices  map[string]string
	DefaultBase string
}

// NewMock 创建 Mock 采集器。
func NewMock(cfg MockConfig) (*Mock, error) {
	w := cfg.Weight
	if !w.IsPositive() {
		w = decimal.NewFromInt(1)
	}
	def, err := decimal.NewFromString(cfg.DefaultBase)
	if err != nil || !def.IsPositive() {
		def = decimal.NewFromInt(65000)
	}
	bases := make(map[string]decimal.Decimal, len(cfg.BasePrices))
	for sym, s := range cfg.BasePrices {
		p, err := decimal.NewFromString(s)
		if err != nil || !p.IsPositive() {
			continue
		}
		bases[sym] = p
	}
	return &Mock{
		weight:      w,
		basePrices:  bases,
		defaultBase: def,
	}, nil
}

func (m *Mock) Name() string { return "mock" }

func (m *Mock) Weight() decimal.Decimal { return m.weight }

func (m *Mock) FetchPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	_ = ctx
	m.mu.Lock()
	m.tick++
	tick := m.tick
	m.mu.Unlock()

	base := m.defaultBase
	if p, ok := m.basePrices[symbol]; ok {
		base = p
	}
	// 确定性小幅波动，便于单测与演示。
	h := fnv.New32a()
	_, _ = h.Write([]byte(symbol))
	_, _ = h.Write([]byte{byte(tick)})
	noise := int64(h.Sum32()%2001) - 1000 // [-1000, 1000]
	delta := decimal.NewFromInt(noise).Div(decimal.NewFromInt(100000))
	return base.Add(base.Mul(delta)), nil
}
