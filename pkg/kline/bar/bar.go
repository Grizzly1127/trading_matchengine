package bar

import (
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
	"github.com/shopspring/decimal"
)

// OHLCV 单根 K 线（开/高/低/收/量）。
type OHLCV struct {
	OpenTimeMs  int64
	Open        decimal.Decimal
	High        decimal.Decimal
	Low         decimal.Decimal
	Close       decimal.Decimal
	Volume      decimal.Decimal
	UpdatedAtMs int64
}

// Clone 深拷贝 OHLCV。
func (b OHLCV) Clone() OHLCV {
	return b
}

// NewFromTrade 用首笔成交初始化 bar。
func NewFromTrade(openTimeMs int64, price, qty decimal.Decimal, tradeTimeMs int64) OHLCV {
	return OHLCV{
		OpenTimeMs:  openTimeMs,
		Open:        price,
		High:        price,
		Low:         price,
		Close:       price,
		Volume:      qty,
		UpdatedAtMs: tradeTimeMs,
	}
}

// ApplyTrade 在同一时间桶内更新 OHLCV。
func (b *OHLCV) ApplyTrade(price, qty decimal.Decimal, tradeTimeMs int64) {
	b.High = decimal.Max(b.High, price)
	b.Low = decimal.Min(b.Low, price)
	b.Close = price
	b.Volume = b.Volume.Add(qty)
	b.UpdatedAtMs = tradeTimeMs
}

// JSON 是 Redis / WS 使用的序列化结构。
type JSON struct {
	Symbol      string `json:"symbol"`
	Interval    string `json:"interval"`
	OpenTimeMs  int64  `json:"open_time_ms"`
	CloseTimeMs int64  `json:"close_time_ms"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Close       string `json:"close"`
	Volume      string `json:"volume"`
	IsClosed    bool   `json:"is_closed"`
	TimestampMs int64  `json:"ts"`
}

// ToJSON 转为推送/缓存 JSON 结构。
func ToJSON(symbol string, iv interval.Interval, b OHLCV, closed bool) JSON {
	closeMs := interval.Interval(iv).CloseTimeMs(b.OpenTimeMs)
	return JSON{
		Symbol:      symbol,
		Interval:    string(iv),
		OpenTimeMs:  b.OpenTimeMs,
		CloseTimeMs: closeMs,
		Open:        b.Open.String(),
		High:        b.High.String(),
		Low:         b.Low.String(),
		Close:       b.Close.String(),
		Volume:      b.Volume.String(),
		IsClosed:    closed,
		TimestampMs: b.UpdatedAtMs,
	}
}

// ParseDecimal 解析十进制字符串。
func ParseDecimal(v string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(v)
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid decimal %q: %w", v, err)
	}
	return d, nil
}
