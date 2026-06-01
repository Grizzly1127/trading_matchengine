package hub_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
)

func TestBroadcastOrder_routesByUserID(t *testing.T) {
	h := hub.New()
	c1 := hub.NewClient(nil)
	c2 := hub.NewClient(nil)
	c1.UserID = 1
	c2.UserID = 2
	c1.SetSubscribed("order", true)
	c2.SetSubscribed("order", true)
	h.Add(c1)
	h.Add(c2)

	h.BroadcastOrder(1, []byte(`{"stream":"order"}`))

	select {
	case <-c1.Send:
	default:
		t.Fatal("user 1 should receive order update")
	}
	select {
	case <-c2.Send:
		t.Fatal("user 2 should not receive user 1 order update")
	default:
	}
}

func TestParseOrderChannel(t *testing.T) {
	uid, ok := hub.ParseOrderChannel("order:42")
	if !ok || uid != 42 {
		t.Fatalf("uid=%d ok=%v", uid, ok)
	}
}
