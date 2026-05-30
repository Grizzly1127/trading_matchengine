package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
)

// FindStalePendingForReject 返回已投递 NewOrder 但仍 PENDING 且超时的订单 ID。
func (r *Repository) FindStalePendingForReject(ctx context.Context, olderThan time.Time, limit int) ([]uint64, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
SELECT o.id
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
	return scanOrderIDs(rows)
}

// RejectStalePending 将超时 PENDING 置为 REJECTED、释放冻结，并写入 Cancel Outbox 通知撮合摘单。
func (r *Repository) RejectStalePending(ctx context.Context, orderID uint64, outboxTopic string) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	order, err := getOrderForUpdate(ctx, tx, orderID)
	if err != nil {
		return false, err
	}
	if order.Status != status.Pending {
		return false, nil
	}
	if err := transitionOrderStatus(ctx, tx, order, status.Rejected, nil); err != nil {
		if err == errCASConflict {
			return false, nil
		}
		return false, err
	}
	current, err := getOrderForUpdate(ctx, tx, order.ID)
	if err != nil {
		return false, err
	}
	if err := r.releaseOrderRemainingFreeze(ctx, tx, current); err != nil {
		return false, err
	}
	if err := insertCancelOutbox(ctx, tx, current, outboxTopic); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}
	return true, nil
}

// FindStuckCancelingForResend 返回 CANCELING 超时且无待投递撤单 Outbox 的订单 ID。
func (r *Repository) FindStuckCancelingForResend(ctx context.Context, olderThan time.Time, limit int) ([]uint64, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
SELECT o.id
FROM orders o
WHERE o.status = $1
  AND o.updated_at < $2
  AND NOT EXISTS (
    SELECT 1 FROM order_outbox ob
    WHERE ob.aggregate_id = o.id
      AND ob.event_type = $3
      AND ob.published_at IS NULL
  )
ORDER BY o.id ASC
LIMIT $4`

	rows, err := r.pool.Query(ctx, q, status.Canceling, olderThan, outbox.EventTypeCancelOrder, limit)
	if err != nil {
		return nil, fmt.Errorf("find stuck canceling: %w", err)
	}
	defer rows.Close()
	return scanOrderIDs(rows)
}

// ResendCancelCommand 为 CANCELING 订单补写撤单 Outbox（新 command_id，幂等由撮合去重 order_id）。
func (r *Repository) ResendCancelCommand(ctx context.Context, orderID uint64, outboxTopic string) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	order, err := getOrderForUpdate(ctx, tx, orderID)
	if err != nil {
		return false, err
	}
	if order.Status != status.Canceling {
		return false, nil
	}
	if err := insertCancelOutbox(ctx, tx, order, outboxTopic); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit tx: %w", err)
	}
	return true, nil
}

// CountStaleUnpublishedOutbox 统计 PENDING 订单下长期未发出的 Outbox 条数（用于告警日志）。
func (r *Repository) CountStaleUnpublishedOutbox(ctx context.Context, olderThan time.Time) (int, error) {
	const q = `
SELECT COUNT(*)
FROM order_outbox ob
INNER JOIN orders o ON o.id = ob.aggregate_id
WHERE o.status = $1
  AND ob.published_at IS NULL
  AND ob.created_at < $2`

	var n int
	err := r.pool.QueryRow(ctx, q, status.Pending, olderThan).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count stale outbox: %w", err)
	}
	return n, nil
}

func scanOrderIDs(rows pgx.Rows) ([]uint64, error) {
	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan order id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}
