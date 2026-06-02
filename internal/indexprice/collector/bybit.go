package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/shopspring/decimal"
)

// Bybit REST 现货 ticker。
type Bybit struct {
	weight     decimal.Decimal
	baseURL    string
	httpClient *http.Client
}

func NewBybit(cfg ExchangeConfig, timeout time.Duration) (*Bybit, error) {
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	w := cfg.Weight
	if !w.IsPositive() {
		w = decimal.NewFromInt(1)
	}
	base := cfg.BaseURL
	if base == "" {
		base = "https://api.bybit.com"
	}
	return &Bybit{
		weight:     w,
		baseURL:    base,
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

func (b *Bybit) Name() string { return "bybit" }

func (b *Bybit) Weight() decimal.Decimal { return b.weight }

func (b *Bybit) FetchPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	inst := ToBybitSymbol(symbol)
	u, err := url.Parse(b.baseURL + "/v5/market/tickers")
	if err != nil {
		return decimal.Decimal{}, err
	}
	q := u.Query()
	q.Set("category", "spot")
	q.Set("symbol", inst)
	u.RawQuery = q.Encode()
	body, err := httpGetJSON(ctx, b.httpClient, u.String(), 256*1024)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("bybit fetch %s: %w", symbol, err)
	}
	return ParseBybitTicker(body)
}
