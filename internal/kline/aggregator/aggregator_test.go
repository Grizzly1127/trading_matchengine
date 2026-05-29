package aggregator

import (
	"testing"

	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
)

func TestApplyTradeBucketRollover(t *testing.T) {
	agg := NewAggregator()
	var closed []ClosedEvent
	agg.SetOnClose(func(ev ClosedEvent) {
		closed = append(closed, ev)
	})

	baseMs := int64(1_700_000_000_000)
	if err := agg.ApplyTrade("BTC-USDT", "100", "1", baseMs); err != nil {
		t.Fatal(err)
	}
	b, ok := agg.SnapshotOpen("BTC-USDT", interval.Min1)
	if !ok {
		t.Fatal("expected open bar")
	}
	if !b.Open.Equal(decimal.RequireFromString("100")) {
		t.Fatalf("open: %s", b.Open)
	}

	if err := agg.ApplyTrade("BTC-USDT", "110", "2", baseMs+10_000); err != nil {
		t.Fatal(err)
	}
	b, _ = agg.SnapshotOpen("BTC-USDT", interval.Min1)
	if !b.High.Equal(decimal.RequireFromString("110")) {
		t.Fatalf("high: %s", b.High)
	}
	if !b.Volume.Equal(decimal.RequireFromString("3")) {
		t.Fatalf("volume: %s", b.Volume)
	}
	closed = filterClosed(closed, interval.Min1)
	if len(closed) != 0 {
		t.Fatalf("unexpected 1m close: %d", len(closed))
	}

	nextMin := baseMs + 60_000
	closed = nil
	if err := agg.ApplyTrade("BTC-USDT", "105", "1", nextMin); err != nil {
		t.Fatal(err)
	}
	closed = filterClosed(closed, interval.Min1)
	if len(closed) != 1 {
		t.Fatalf("want 1 closed 1m bar, got %d", len(closed))
	}
	if closed[0].Interval != interval.Min1 {
		t.Fatalf("interval: %s", closed[0].Interval)
	}
	if !closed[0].Bar.Close.Equal(decimal.RequireFromString("110")) {
		t.Fatalf("closed close: %s", closed[0].Bar.Close)
	}
}

func filterClosed(in []ClosedEvent, iv interval.Interval) []ClosedEvent {
	out := make([]ClosedEvent, 0, len(in))
	for _, ev := range in {
		if ev.Interval == iv {
			out = append(out, ev)
		}
	}
	return out
}
