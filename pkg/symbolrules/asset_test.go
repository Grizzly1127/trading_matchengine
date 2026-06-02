package symbolrules

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestAssetRegistry_RoundDown(t *testing.T) {
	reg, err := DefaultAssetRegistry()
	if err != nil {
		t.Fatal(err)
	}
	got := reg.RoundDown("USDT", decimal.RequireFromString("10.123456789"))
	want := decimal.RequireFromString("10.12345678")
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestAssetRegistry_RoundUp(t *testing.T) {
	reg, err := DefaultAssetRegistry()
	if err != nil {
		t.Fatal(err)
	}
	got := reg.RoundUp("USDT", decimal.RequireFromString("10.123456789"))
	want := decimal.RequireFromString("10.12345679")
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}
