package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/shopspring/decimal"
)

// Binance REST 现货 ticker 价格。
type Binance struct {
	weight     decimal.Decimal
	baseURL    string
	httpClient *http.Client
}

type ExchangeConfig struct {
	Enabled bool
	Weight  decimal.Decimal
	BaseURL string
}

func NewBinance(cfg ExchangeConfig, timeout time.Duration) (*Binance, error) {
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	w := cfg.Weight
	if !w.IsPositive() {
		w = decimal.NewFromInt(1)
	}
	base := cfg.BaseURL
	if base == "" {
		base = "https://api.binance.com"
	}
	return &Binance{
		weight:     w,
		baseURL:    base,
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

func (b *Binance) Name() string { return "binance" }

func (b *Binance) Weight() decimal.Decimal { return b.weight }

func (b *Binance) FetchPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	inst := ToBinanceSymbol(symbol)
	u, err := url.Parse(b.baseURL + "/api/v3/ticker/price")
	if err != nil {
		return decimal.Decimal{}, err
	}
	q := u.Query()
	q.Set("symbol", inst)
	u.RawQuery = q.Encode()
	body, err := httpGetJSON(ctx, b.httpClient, u.String(), 64*1024)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("binance fetch %s: %w", symbol, err)
	}
	return ParseBinanceTickerPrice(body)
}
