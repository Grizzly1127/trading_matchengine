package hub

import "testing"

func TestClientSubscription(t *testing.T) {
	c := &Client{subs: map[string]struct{}{}}
	c.SetSubscribed("depth:BTC-USDT", true)
	if !c.IsSubscribed("depth:BTC-USDT") {
		t.Fatal("expected subscribed")
	}
	c.SetSubscribed("depth:BTC-USDT", false)
	if c.IsSubscribed("depth:BTC-USDT") {
		t.Fatal("expected unsubscribed")
	}
}
