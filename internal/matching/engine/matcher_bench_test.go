package engine_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/engine"
	"github.com/shopspring/decimal"
)

func benchLimitOrder(side engine.Side, price int64, cross bool) engine.Order {
	p := decimal.NewFromInt(price)
	if cross && side == engine.SideBuy {
		return engine.Order{
			Side: side, Type: engine.OrderTypeLimit, Price: p,
			Quantity: decimal.NewFromInt(1),
		}
	}
	if !cross && side == engine.SideBuy {
		return engine.Order{
			Side: side, Type: engine.OrderTypeLimit, Price: p,
			Quantity: decimal.NewFromInt(1),
		}
	}
	return engine.Order{
		Side: side, Type: engine.OrderTypeLimit, Price: p,
		Quantity: decimal.NewFromInt(1),
	}
}

// BenchmarkMatch_restingBuy 纯挂单（不交叉）。
func BenchmarkMatch_restingBuy(b *testing.B) {
	sh := newTestShard()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := benchLimitOrder(engine.SideBuy, 99, false)
		o.OrderID = uint64(i + 1)
		o.ClientOrderID = "bench-rest"
		o.Symbol = "BTC-USDT"
		_, _ = sh.Match(o, uint64(i+1))
	}
}

// BenchmarkMatch_takeSell 预置卖盘后买单吃单。
func BenchmarkMatch_takeSell(b *testing.B) {
	sh := newTestShard()
	_, _ = sh.Match(engine.Order{
		OrderID: 1, Symbol: "BTC-USDT", Side: engine.SideSell, Type: engine.OrderTypeLimit,
		Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1_000_000),
	}, 1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := benchLimitOrder(engine.SideBuy, 100, true)
		o.OrderID = uint64(i + 2)
		o.ClientOrderID = "bench-take"
		o.Symbol = "BTC-USDT"
		_, _ = sh.Match(o, uint64(i+2))
	}
}

// BenchmarkMatch_alternateFill 买卖交替，模拟盘口变动。
func BenchmarkMatch_alternateFill(b *testing.B) {
	sh := newTestShard()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			o := engine.Order{
				OrderID: uint64(i + 1), Symbol: "BTC-USDT",
				Side: engine.SideSell, Type: engine.OrderTypeLimit,
				Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
			}
			_, _ = sh.Match(o, uint64(i+1))
		} else {
			o := engine.Order{
				OrderID: uint64(i + 1), Symbol: "BTC-USDT",
				Side: engine.SideBuy, Type: engine.OrderTypeLimit,
				Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
			}
			_, _ = sh.Match(o, uint64(i+1))
		}
	}
}
