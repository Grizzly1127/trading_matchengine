package collector

import (
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/config"
	"github.com/shopspring/decimal"
)

// Build 按配置构造已启用的采集器列表。
func Build(cfg config.Config) ([]Collector, error) {
	timeout := time.Duration(cfg.FetchTimeoutMs) * time.Millisecond
	var out []Collector

	if cfg.Sources.Mock.Enabled {
		mock, err := NewMock(MockConfig{
			Weight:      parseWeight(cfg.Sources.Mock.Weight),
			BasePrices:  cfg.Mock.BasePrices,
			DefaultBase: cfg.Mock.DefaultBase,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, mock)
	}
	if cfg.Sources.Binance.Enabled {
		c, err := NewBinance(ExchangeConfig{
			Enabled: true,
			Weight:  parseWeight(cfg.Sources.Binance.Weight),
			BaseURL: cfg.Sources.Binance.BaseURL,
		}, timeout)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if cfg.Sources.OKX.Enabled {
		c, err := NewOKX(ExchangeConfig{
			Enabled: true,
			Weight:  parseWeight(cfg.Sources.OKX.Weight),
			BaseURL: cfg.Sources.OKX.BaseURL,
		}, timeout)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if cfg.Sources.Bybit.Enabled {
		c, err := NewBybit(ExchangeConfig{
			Enabled: true,
			Weight:  parseWeight(cfg.Sources.Bybit.Weight),
			BaseURL: cfg.Sources.Bybit.BaseURL,
		}, timeout)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("collector: no enabled sources")
	}
	return out, nil
}

func parseWeight(s string) decimal.Decimal {
	if s == "" {
		return decimal.NewFromInt(1)
	}
	d, err := decimal.NewFromString(s)
	if err != nil || !d.IsPositive() {
		return decimal.NewFromInt(1)
	}
	return d
}
