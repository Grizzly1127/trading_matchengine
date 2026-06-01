package limits

import "testing"

func TestSymbolFromChannel(t *testing.T) {
	tests := []struct {
		ch   string
		sym  string
		want bool
	}{
		{"ticker:BTC-USDT", "BTC-USDT", true},
		{"depth:ETH-USDT", "ETH-USDT", true},
		{"kline:BTC-USDT:1m", "BTC-USDT", true},
		{"ticker@all", "", false},
		{"ticker@all:USDT", "", false},
		{"order", "", false},
		{"order:1", "", false},
	}
	for _, tt := range tests {
		sym, ok := SymbolFromChannel(tt.ch)
		if ok != tt.want || sym != tt.sym {
			t.Fatalf("%q => %q,%v want %q,%v", tt.ch, sym, ok, tt.sym, tt.want)
		}
	}
}

func TestMergeSymbolCount(t *testing.T) {
	n := MergeSymbolCount([]string{"ticker:BTC-USDT"}, []string{"depth:BTC-USDT", "ticker:ETH-USDT"})
	if n != 2 {
		t.Fatalf("count=%d want 2", n)
	}
}
