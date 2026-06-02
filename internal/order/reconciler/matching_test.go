package reconciler

import (
	"testing"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

func TestShouldRejectAfterMatching(t *testing.T) {
	if shouldRejectAfterMatching(matchingv1.OrderPresence_ORDER_PRESENCE_UNKNOWN) {
		// UNKNOWN：撮合未见过，允许超时拒单
	} else {
		t.Fatal("unknown should allow reject")
	}
	if shouldRejectAfterMatching(matchingv1.OrderPresence_ORDER_PRESENCE_IN_ORDERBOOK) {
		t.Fatal("in book should skip reject")
	}
	if shouldRejectAfterMatching(matchingv1.OrderPresence_ORDER_PRESENCE_KNOWN_NOT_IN_ORDERBOOK) {
		t.Fatal("known not in book should skip reject")
	}
}
