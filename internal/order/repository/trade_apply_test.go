package repository

import (
	"testing"

	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
	"github.com/shopspring/decimal"
)

func TestShouldReleaseFreezeAfterTrade(t *testing.T) {
	orderQty := decimal.NewFromInt(1)
	cases := []struct {
		name     string
		filled   string
		fill     string
		status   string
		release  bool
	}{
		{"partial on book", "0", "0.3", status.Partial, false},
		{"full fill", "0", "1", status.Filled, true},
		{"accumulated to full", "0.5", "0.5", status.Partial, true},
		{"filled status before qty caught up", "0", "0.5", status.Filled, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur, _ := decimal.NewFromString(tc.filled)
			fill, _ := decimal.NewFromString(tc.fill)
			newFilled := cur.Add(fill)
			if newFilled.GreaterThan(orderQty) {
				newFilled = orderQty
			}
			got := !newFilled.LessThan(orderQty) || tc.status == status.Filled
			if got != tc.release {
				t.Fatalf("release=%v want %v (newFilled=%s status=%s)", got, tc.release, newFilled, tc.status)
			}
		})
	}
}
