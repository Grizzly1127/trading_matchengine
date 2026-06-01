package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
)

// CountUnpublishedOutbox 未投递 Outbox 条数（全表）。
func (r *Repository) CountUnpublishedOutbox(ctx context.Context) (int, error) {
	const q = `SELECT COUNT(*) FROM order_outbox WHERE published_at IS NULL`
	var n int
	if err := r.pool.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("count unpublished outbox: %w", err)
	}
	return n, nil
}

// MaxStuckPendingSeconds 已发命令但仍 PENDING 的最长等待秒数；无则 0。
func (r *Repository) MaxStuckPendingSeconds(ctx context.Context) (float64, error) {
	const q = `
SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (now() - o.updated_at))), 0)
FROM orders o
WHERE o.status = $1
  AND EXISTS (
    SELECT 1 FROM order_outbox ob
    WHERE ob.aggregate_id = o.id
      AND ob.event_type = 'NewOrder'
      AND ob.published_at IS NOT NULL
  )`
	var sec float64
	if err := r.pool.QueryRow(ctx, q, status.Pending).Scan(&sec); err != nil {
		return 0, fmt.Errorf("max stuck pending seconds: %w", err)
	}
	return sec, nil
}

// StalePendingOrder 超时待补偿的 PENDING 订单。
type StalePendingOrder struct {
	ID     uint64
	Symbol string
}

// FindStalePendingForReject 返回已投递 NewOrder 但仍 PENDING 且超时的订单。
func (r *Repository) FindStalePendingForReject(ctx context.Context, olderThan time.Time, limit int) ([]StalePendingOrder, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
SELECT o.id, o.symbol
FROM orders o
WHERE o.status = $1
  AND o.updated_at < $2
  AND EXISTS (
    SELECT 1 FROM order_outbox ob
    WHERE ob.aggregate_id = o.id
      AND ob.event_type = $3
      AND ob.published_at IS NOT NULL
  )
ORDER BY o.id ASC
LIMIT $4`

	rows, err := r.pool.Query(ctx, q, status.Pending, olderThan, outbox.EventTypeNewOrder, limit)
	if err != nil {
		return nil, fmt.Errorf("find stale pending: %w", err)
	}
	defer rows.Close()

	var out []StalePendingOrder
	for rows.Next() {
		var o StalePendingOrder
		if err := rows.Scan(&o.ID, &o.Symbol); err != nil {
			return nil, fmt.Errorf("scan stale pending: %w", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
