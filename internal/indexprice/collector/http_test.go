package collector

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseBinanceTickerPrice(t *testing.T) {
	p, err := ParseBinanceTickerPrice([]byte(`{"symbol":"BTCUSDT","price":"65000.12"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !p.Equal(decimal.RequireFromString("65000.12")) {
		t.Fatalf("got %s", p)
	}
}

func TestParseOKXTicker(t *testing.T) {
	body := []byte(`{"code":"0","data":[{"instId":"BTC-USDT","last":"65001"}]}`)
	p, err := ParseOKXTicker(body)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Equal(decimal.NewFromInt(65001)) {
		t.Fatalf("got %s", p)
	}
}

func TestParseBybitTicker(t *testing.T) {
	body := []byte(`{"retCode":0,"result":{"list":[{"symbol":"BTCUSDT","lastPrice":"64999.5"}]}}`)
	p, err := ParseBybitTicker(body)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Equal(decimal.RequireFromString("64999.5")) {
		t.Fatalf("got %s", p)
	}
}
