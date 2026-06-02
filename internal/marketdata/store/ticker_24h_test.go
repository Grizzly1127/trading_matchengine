package store

import "testing"

func TestTicker24hOHLCAndChangePercent(t *testing.T) {
	t.Parallel()
	st := New()
	base := int64(1_000_000)
	must(t, st.ApplyTrade("BTC-USDT", "100", "1", base))
	must(t, st.ApplyTrade("BTC-USDT", "110", "1", base+1000))
	must(t, st.ApplyTrade("BTC-USDT", "90", "1", base+2000))

	tk, ok := st.SnapshotTicker("BTC-USDT")
	if !ok {
		t.Fatal("ticker not found")
	}
	if tk.OpenPrice.String() != "100" || tk.HighPrice.String() != "110" || tk.LowPrice.String() != "90" {
		t.Fatalf("ohlc: open=%s high=%s low=%s", tk.OpenPrice, tk.HighPrice, tk.LowPrice)
	}
	if tk.LastPrice.String() != "90" {
		t.Fatalf("last=%s", tk.LastPrice)
	}
	if tk.Volume.String() != "3" {
		t.Fatalf("volume=%s", tk.Volume)
	}
	// (90-100)/100*100 = -10%
	if FormatPercent(tk.PriceChangePercent) != "-10.00" {
		t.Fatalf("change%%=%s", FormatPercent(tk.PriceChangePercent))
	}
}

func TestTicker24hRollingWindowPrunesOldTrades(t *testing.T) {
	t.Parallel()
	st := New()
	oldMs := int64(1_000)
	must(t, st.ApplyTrade("BTC-USDT", "50", "1", oldMs))

	afterWindow := oldMs + tickerWindowMs + 1
	must(t, st.ApplyTrade("BTC-USDT", "200", "2", afterWindow))

	tk, ok := st.SnapshotTicker("BTC-USDT")
	if !ok {
		t.Fatal("ticker not found")
	}
	if tk.OpenPrice.String() != "200" || tk.LastPrice.String() != "200" {
		t.Fatalf("after prune open/last=%s/%s want 200", tk.OpenPrice, tk.LastPrice)
	}
	if tk.Volume.String() != "2" {
		t.Fatalf("volume=%s want 2", tk.Volume)
	}
	if FormatPercent(tk.PriceChangePercent) != "0" {
		t.Fatalf("change%%=%s want 0", FormatPercent(tk.PriceChangePercent))
	}
}
