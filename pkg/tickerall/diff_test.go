package tickerall

import (
	"encoding/json"
	"testing"
)

func TestDiffOnlyChangedFields(t *testing.T) {
	prev := CompactMapFromItems([]CompactItem{
		{S: "BTC-USDT", P: "1", V: "10", Q: "10", C: "0.1"},
		{S: "ETH-USDT", P: "2", V: "20", Q: "20", C: "0.2"},
	})
	curr := CompactMapFromItems([]CompactItem{
		{S: "BTC-USDT", P: "1.1", V: "10", Q: "10", C: "0.1"},
		{S: "ETH-USDT", P: "2", V: "20", Q: "20", C: "0.2"},
	})
	delta := Diff(prev, curr)
	if len(delta) != 1 {
		t.Fatalf("len=%d want 1", len(delta))
	}
	if delta[0].S != "BTC-USDT" || delta[0].P != "1.1" {
		t.Fatalf("delta=%+v", delta[0])
	}
	if delta[0].V != "" || delta[0].Q != "" {
		t.Fatalf("unexpected extra fields: %+v", delta[0])
	}
}

func TestDiffNewSymbol(t *testing.T) {
	prev := CompactMapFromItems([]CompactItem{{S: "BTC-USDT", P: "1"}})
	curr := CompactMapFromItems([]CompactItem{
		{S: "BTC-USDT", P: "1"},
		{S: "ETH-USDT", P: "2", V: "3"},
	})
	delta := Diff(prev, curr)
	if len(delta) != 1 || delta[0].S != "ETH-USDT" {
		t.Fatalf("delta=%+v", delta)
	}
}

func TestWSSnapshotFromRedisREST(t *testing.T) {
	rest := []byte(`{
		"snapshot_id":"snap-abc",
		"snapshot_time":1716192000123,
		"count":1,
		"items":[{"symbol":"BTC-USDT","last_price":"1","volume":"2","quote_volume":"3","price_change_percent":"4"}]
	}`)
	frame, err := WSSnapshotFromRedisREST("ticker@all:USDT", rest)
	if err != nil {
		t.Fatal(err)
	}
	var f Frame
	if err := json.Unmarshal(frame, &f); err != nil {
		t.Fatal(err)
	}
	if f.Type != TypeSnapshot || f.Stream != "ticker@all:USDT" || f.SnapshotID != "snap-abc" {
		t.Fatalf("frame=%+v", f)
	}
}
