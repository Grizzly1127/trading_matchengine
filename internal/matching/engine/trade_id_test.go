package engine

import "testing"

func TestDeriveTradeID_fitsInt64(t *testing.T) {
	id := DeriveTradeID(^uint64(0), ^uint64(0), ^uint64(0))
	const maxInt64 = uint64(1<<63 - 1)
	if id > maxInt64 {
		t.Fatalf("trade_id=%d exceeds int64 max", id)
	}
}
