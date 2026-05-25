package symbol

import "testing"

func TestParsePair(t *testing.T) {
	p, err := ParsePair("BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if p.Base != "BTC" || p.Quote != "USDT" {
		t.Fatalf("pair=%+v", p)
	}
}

func TestParsePair_Invalid(t *testing.T) {
	if _, err := ParsePair("BTCUSDT"); err == nil {
		t.Fatal("expected error")
	}
}
