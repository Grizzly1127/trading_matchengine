package convert

import (
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

func TestParseSide(t *testing.T) {
	s, err := ParseSide("buy")
	if err != nil || s != commonv1.Side_SIDE_BUY {
		t.Fatalf("side=%v err=%v", s, err)
	}
	if SideToString(s) != "BUY" {
		t.Fatalf("string=%q", SideToString(s))
	}
}

func TestParseOrderType(t *testing.T) {
	typ, err := ParseOrderType("MARKET")
	if err != nil || typ != commonv1.OrderType_ORDER_TYPE_MARKET {
		t.Fatalf("type=%v err=%v", typ, err)
	}
}
