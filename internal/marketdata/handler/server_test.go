package handler

import (
	"context"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
)

func TestServerGetOrderBookAndTicker(t *testing.T) {
	t.Parallel()

	st := store.New()
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 1, "BUY", "65000", "1.5"))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 2, "SELL", "65010", "0.7"))
	must(t, st.ApplyTrade("BTC-USDT", "65005", "2", 1000))
	srv := NewServer(st)

	book, err := srv.GetOrderBook(context.Background(), &marketdatav1.GetOrderBookRequest{
		Symbol: "BTC-USDT",
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("GetOrderBook: %v", err)
	}
	if len(book.GetBids()) != 1 || book.GetBids()[0].GetPrice().GetValue() != "65000" {
		t.Fatalf("unexpected book: %+v", book)
	}

	ticker, err := srv.GetTicker(context.Background(), &marketdatav1.GetTickerRequest{Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("GetTicker: %v", err)
	}
	if ticker.GetTicker().GetLastPrice().GetValue() != "65005" {
		t.Fatalf("unexpected ticker: %+v", ticker.GetTicker())
	}
	if ticker.GetTicker().GetOpenPrice().GetValue() != "65005" {
		t.Fatalf("open should equal first trade: %+v", ticker.GetTicker())
	}
	if ticker.GetTicker().GetPriceChangePercent().GetValue() != "0" {
		t.Fatalf("single trade change%%: %+v", ticker.GetTicker().GetPriceChangePercent())
	}
}

func TestServerGetTickerOHLC(t *testing.T) {
	t.Parallel()
	st := store.New()
	base := int64(2_000_000)
	must(t, st.ApplyTrade("BTC-USDT", "100", "1", base))
	must(t, st.ApplyTrade("BTC-USDT", "120", "1", base+1))
	must(t, st.ApplyTrade("BTC-USDT", "80", "1", base+2))
	srv := NewServer(st)

	resp, err := srv.GetTicker(context.Background(), &marketdatav1.GetTickerRequest{Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("GetTicker: %v", err)
	}
	tk := resp.GetTicker()
	if tk.GetHighPrice().GetValue() != "120" || tk.GetLowPrice().GetValue() != "80" {
		t.Fatalf("high/low: %+v", tk)
	}
	if tk.GetPriceChangePercent().GetValue() != "-20.00" {
		t.Fatalf("change%%=%s", tk.GetPriceChangePercent().GetValue())
	}
}

func TestServerGetReferencePrice(t *testing.T) {
	t.Parallel()

	st := store.New()
	must(t, st.ApplyTrade("BTC-USDT", "64900", "1", 1000))
	must(t, st.ApplyOrderBookAccepted("BTC-USDT", 1, "SELL", "65010", "0.7"))
	srv := NewServer(st)

	ref, err := srv.GetReferencePrice(context.Background(), &marketdatav1.GetReferencePriceRequest{
		Symbol: "BTC-USDT",
		Kind:   marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_BEST_ASK,
	})
	if err != nil {
		t.Fatalf("GetReferencePrice: %v", err)
	}
	if ref.GetPrice().GetValue() != "65010" {
		t.Fatalf("unexpected reference price: %+v", ref)
	}
}

func TestServerGetTickerAllSnapshotNotModified(t *testing.T) {
	t.Parallel()
	st := store.New()
	must(t, st.ApplyTrade("BTC-USDT", "65000", "1", 1000))
	srv := NewServer(st)

	first, err := srv.GetTickerAllSnapshot(context.Background(), &marketdatav1.GetTickerAllSnapshotRequest{
		QuoteAsset: "USDT",
	})
	if err != nil {
		t.Fatalf("GetTickerAllSnapshot: %v", err)
	}
	if first.GetSnapshotId() == "" {
		t.Fatal("snapshot id should not be empty")
	}

	second, err := srv.GetTickerAllSnapshot(context.Background(), &marketdatav1.GetTickerAllSnapshotRequest{
		QuoteAsset: "USDT",
		SnapshotId: first.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetTickerAllSnapshot not modified: %v", err)
	}
	if !second.GetNotModified() {
		t.Fatal("expected not_modified=true")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
