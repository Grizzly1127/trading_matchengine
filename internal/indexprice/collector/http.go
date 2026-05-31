package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shopspring/decimal"
)

const defaultHTTPTimeout = 3 * time.Second

// httpGetJSON 拉取 JSON 响应体。
func httpGetJSON(ctx context.Context, client *http.Client, url string, maxBytes int64) ([]byte, error) {
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	lim := maxBytes
	if lim <= 0 {
		lim = 1 << 20
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, lim))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func parseDecimalString(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("invalid decimal %q: %w", s, err)
	}
	if !d.IsPositive() {
		return decimal.Decimal{}, fmt.Errorf("non-positive price %q", s)
	}
	return d, nil
}

// ParseBinanceTickerPrice 解析 Binance /api/v3/ticker/price 响应。
func ParseBinanceTickerPrice(body []byte) (decimal.Decimal, error) {
	var v struct {
		Price string `json:"price"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return decimal.Decimal{}, err
	}
	return parseDecimalString(v.Price)
}

// ParseOKXTicker 解析 OKX /api/v5/market/ticker 响应。
func ParseOKXTicker(body []byte) (decimal.Decimal, error) {
	var v struct {
		Code string `json:"code"`
		Data []struct {
			Last string `json:"last"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return decimal.Decimal{}, err
	}
	if v.Code != "0" || len(v.Data) == 0 {
		return decimal.Decimal{}, fmt.Errorf("okx: empty or error response")
	}
	return parseDecimalString(v.Data[0].Last)
}

// ParseBybitTicker 解析 Bybit /v5/market/tickers 响应。
func ParseBybitTicker(body []byte) (decimal.Decimal, error) {
	var v struct {
		RetCode int `json:"retCode"`
		Result  struct {
			List []struct {
				LastPrice string `json:"lastPrice"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return decimal.Decimal{}, err
	}
	if v.RetCode != 0 || len(v.Result.List) == 0 {
		return decimal.Decimal{}, fmt.Errorf("bybit: empty or error response")
	}
	return parseDecimalString(v.Result.List[0].LastPrice)
}
