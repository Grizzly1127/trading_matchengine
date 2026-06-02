package tickerall

import "testing"

func TestIsWSFrameSnapshotAndDelta(t *testing.T) {
	snap, err := MarshalSnapshot("ticker@all:USDT", "snap-1", 100, 1, []CompactItem{{S: "BTC-USDT", P: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !IsWSFrame(snap) {
		t.Fatal("expected snapshot frame")
	}
	delta, err := MarshalDelta("ticker@all:USDT", "snap-1", 101, []CompactItem{{S: "BTC-USDT", P: "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if !IsWSFrame(delta) {
		t.Fatal("expected delta frame")
	}
	hb, err := MarshalHeartbeat("ticker@all:USDT", "snap-1", 102)
	if err != nil {
		t.Fatal(err)
	}
	if !IsWSFrame(hb) {
		t.Fatal("expected heartbeat frame")
	}
	if IsWSFrame([]byte(`{"symbol":"BTC-USDT"}`)) {
		t.Fatal("rest json should not pass")
	}
}
