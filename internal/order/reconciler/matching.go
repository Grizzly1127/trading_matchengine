package reconciler

import (
	"context"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// MatchingAdmin 撮合对账（§4.5）。
type MatchingAdmin interface {
	GetOrderPresence(ctx context.Context, symbol string, orderID uint64) (matchingv1.OrderPresence, error)
}

// shouldRejectAfterMatching 根据撮合侧存在性决定是否仍执行超时拒单。
func shouldRejectAfterMatching(p matchingv1.OrderPresence) bool {
	switch p {
	case matchingv1.OrderPresence_ORDER_PRESENCE_IN_ORDERBOOK,
		matchingv1.OrderPresence_ORDER_PRESENCE_KNOWN_NOT_IN_ORDERBOOK:
		return false
	default:
		return true
	}
}
