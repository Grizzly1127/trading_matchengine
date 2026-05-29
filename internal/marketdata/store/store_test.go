package store

import "testing"

func TestSnapshotOrderBookReturnsSortedTopLevels(t *testing.T) {
	t.Parallel()

	st := New()
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 1, "BUY", "65000", "1.5"))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 2, "BUY", "65010", "0.2"))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 3, "SELL", "65020", "2"))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 4, "SELL", "65015", "0.3"))

	snap, ok := st.SnapshotOrderBook("BTC-USDT", 1)
	if !ok {
		t.Fatal("snapshot not found")
	}
	if snap.LastUpdateID == 0 {
		t.Fatal("last_update_id should increase after book changes")
	}
	if len(snap.Bids) != 1 || snap.Bids[0].Price != "65010" || snap.Bids[0].Quantity != "0.2" {
		t.Fatalf("unexpected bids: %+v", snap.Bids)
	}
	if len(snap.Asks) != 1 || snap.Asks[0].Price != "65015" || snap.Asks[0].Quantity != "0.3" {
		t.Fatalf("unexpected asks: %+v", snap.Asks)
	}
}

func TestSnapshotTickerAllFiltersByQuoteAsset(t *testing.T) {
	t.Parallel()

	st := New()
	must(t, st.ApplyTrade("BTC-USDT", "65000", "2", 1000))
	must(t, st.ApplyTrade("ETH-USDT", "3000", "4", 1001))
	must(t, st.ApplyTrade("ETH-BTC", "0.05", "8", 1002))

	items := st.SnapshotTickerAll("USDT")
	if len(items) != 2 {
		t.Fatalf("want 2 USDT tickers, got %d: %+v", len(items), items)
	}
	if items[0].Symbol != "BTC-USDT" || items[1].Symbol != "ETH-USDT" {
		t.Fatalf("tickers should be sorted by symbol: %+v", items)
	}
}

func TestReferencePriceUsesBestAskThenLast(t *testing.T) {
	t.Parallel()

	st := New()
	must(t, st.ApplyTrade("BTC-USDT", "64900", "1", 1000))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 1, "SELL", "65000", "1"))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 2, "SELL", "64950", "1"))

	ref, ok := st.ReferencePrice("BTC-USDT", ReferencePriceBestAsk)
	if !ok {
		t.Fatal("reference price not found")
	}
	if ref.Price != "64950" || ref.Kind != ReferencePriceBestAsk {
		t.Fatalf("unexpected best ask: %+v", ref)
	}

	ref, ok = st.ReferencePrice("BTC-USDT", ReferencePriceLast)
	if !ok {
		t.Fatal("last price not found")
	}
	if ref.Price != "64900" || ref.Kind != ReferencePriceLast {
		t.Fatalf("unexpected last price: %+v", ref)
	}
}

func TestBuildTickerAllSnapshotIDStableForSameContent(t *testing.T) {
	t.Parallel()
	st := New()
	must(t, st.ApplyTrade("BTC-USDT", "65000", "2", 1000))
	s1 := st.BuildTickerAllSnapshot("USDT")
	s2 := st.BuildTickerAllSnapshot("USDT")
	if s1.SnapshotID == "" {
		t.Fatal("snapshot_id should not be empty")
	}
	if s1.SnapshotID != s2.SnapshotID {
		t.Fatalf("snapshot id should be stable: %s vs %s", s1.SnapshotID, s2.SnapshotID)
	}

	must(t, st.ApplyTrade("BTC-USDT", "65001", "1", 1001))
	s3 := st.BuildTickerAllSnapshot("USDT")
	if s3.SnapshotID == s1.SnapshotID {
		t.Fatalf("snapshot id should change after content update: %s", s3.SnapshotID)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
