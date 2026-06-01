package store

import (
	"github.com/shopspring/decimal"
)

const tickerWindowMs int64 = 24 * 60 * 60 * 1000

type tradeTick struct {
	TimeMs int64
	Price  decimal.Decimal
	Qty    decimal.Decimal
}

func (st *SymbolState) applyTrade24h(price, qty decimal.Decimal, tradeTimeMs int64) {
	st.trades24h = append(st.trades24h, tradeTick{TimeMs: tradeTimeMs, Price: price, Qty: qty})
	cutoff := tradeTimeMs - tickerWindowMs
	// 丢弃窗口外成交（假定按时间递增写入；乱序时全量重算仍正确）。
	i := 0
	for i < len(st.trades24h) && st.trades24h[i].TimeMs < cutoff {
		i++
	}
	if i > 0 {
		st.trades24h = append([]tradeTick(nil), st.trades24h[i:]...)
	}
	recomputeTicker24h(&st.Ticker, st.trades24h, tradeTimeMs)
}

func recomputeTicker24h(t *TickerState, ticks []tradeTick, updatedAtMs int64) {
	t.UpdatedAtMs = updatedAtMs
	if len(ticks) == 0 {
		t.LastPrice = decimal.Zero
		t.OpenPrice = decimal.Zero
		t.HighPrice = decimal.Zero
		t.LowPrice = decimal.Zero
		t.Volume = decimal.Zero
		t.QuoteVolume = decimal.Zero
		t.PriceChangePercent = decimal.Zero
		return
	}

	open := ticks[0].Price
	high := ticks[0].Price
	low := ticks[0].Price
	last := ticks[len(ticks)-1].Price
	vol := decimal.Zero
	qvol := decimal.Zero

	for _, tk := range ticks {
		if tk.Price.GreaterThan(high) {
			high = tk.Price
		}
		if tk.Price.LessThan(low) {
			low = tk.Price
		}
		vol = vol.Add(tk.Qty)
		qvol = qvol.Add(tk.Price.Mul(tk.Qty))
	}

	t.OpenPrice = open
	t.HighPrice = high
	t.LowPrice = low
	t.LastPrice = last
	t.Volume = vol
	t.QuoteVolume = qvol
	t.PriceChangePercent = priceChangePercent(open, last)
}

// priceChangePercent 计算 (last-open)/open*100，open 为 0 时返回 0。
func priceChangePercent(open, last decimal.Decimal) decimal.Decimal {
	if open.IsZero() {
		return decimal.Zero
	}
	return last.Sub(open).Div(open).Mul(decimal.NewFromInt(100))
}

// FormatDecimal 行情对外展示（去掉无意义尾零）。
func FormatDecimal(d decimal.Decimal) string {
	if d.IsZero() {
		return "0"
	}
	return d.String()
}

// FormatPercent 涨跌幅百分比字符串（保留 2 位小数）。
func FormatPercent(d decimal.Decimal) string {
	if d.IsZero() {
		return "0"
	}
	return d.StringFixed(2)
}
