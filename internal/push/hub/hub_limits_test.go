package hub_test

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/limits"
)

func TestCanRegisterConnectionLimit(t *testing.T) {
	h := hub.NewWithLimits(limits.Config{
		RetailMaxConnections:          2,
		RetailMaxSymbolsPerConnection: 50,
		MarketMakerMaxConnections:     2,
	})
	subject := "user-a"

	c1 := hub.NewClient(nil)
	c2 := hub.NewClient(nil)
	if err := h.Register(c1, subject, false); err != nil {
		t.Fatal(err)
	}
	if err := h.Register(c2, subject, false); err != nil {
		t.Fatal(err)
	}
	if h.CanRegister(subject, false) {
		t.Fatal("third connection should be denied")
	}
	h.Remove(c1)
	if !h.CanRegister(subject, false) {
		t.Fatal("should allow after disconnect")
	}
}
