package symbolrules

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestValidateQuantity_rejectsExcessPrecision(t *testing.T) {
	sp := DefaultBTCUSDTSpec()
	_, err := sp.ValidateQuantity(decimal.RequireFromString("1.0000001"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCeilPrice_roundsUpToTick(t *testing.T) {
	sp := DefaultBTCUSDTSpec()
	got, err := sp.CeilPrice(decimal.RequireFromString("100.001"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "100.01" {
		t.Fatalf("got %q want 100.01", got)
	}
}

func TestParsePair(t *testing.T) {
	p, err := ParsePair("BTC-USDT")
	if err != nil || p.Base != "BTC" || p.Quote != "USDT" {
		t.Fatalf("pair=%+v err=%v", p, err)
	}
}
