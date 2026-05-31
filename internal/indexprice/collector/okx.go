package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/shopspring/decimal"
)

// OKX REST 现货 ticker。
type OKX struct {
	weight     decimal.Decimal
	baseURL    string
	httpClient *http.Client
}

func NewOKX(cfg ExchangeConfig, timeout time.Duration) (*OKX, error) {
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	w := cfg.Weight
	if !w.IsPositive() {
		w = decimal.NewFromInt(1)
	}
	base := cfg.BaseURL
	if base == "" {
		base = "https://www.okx.com"
	}
	return &OKX{
		weight:     w,
		baseURL:    base,
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

func (o *OKX) Name() string { return "okx" }

func (o *OKX) Weight() decimal.Decimal { return o.weight }

func (o *OKX) FetchPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	inst := ToOKXInstID(symbol)
	u, err := url.Parse(o.baseURL + "/api/v5/market/ticker")
	if err != nil {
		return decimal.Decimal{}, err
	}
	q := u.Query()
	q.Set("instId", inst)
	u.RawQuery = q.Encode()
	body, err := httpGetJSON(ctx, o.httpClient, u.String(), 128*1024)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("okx fetch %s: %w", symbol, err)
	}
	return ParseOKXTicker(body)
}
