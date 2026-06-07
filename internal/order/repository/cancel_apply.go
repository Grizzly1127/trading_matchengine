package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
)

// ErrOrderNotCancelable 表示订单当前状态不可撤单。
var ErrOrderNotCancelable = fmt.Errorf("order not cancelable")

// BeginCancel 将订单置为 CANCELING 并写入撤单 Outbox。
func (r *Repository) BeginCancel(ctx context.Context, userID, orderID uint64, outboxTopic string) (*Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	order, err := getOrderForUpdateByUser(ctx, tx, userID, orderID)
	if err != nil {
		return nil, err
	}

	switch order.Status {
	case status.Canceling, status.Canceled:
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit tx: %w", err)
		}
		return order, nil
	case status.Filled, status.Rejected:
		return nil, fmt.Errorf("%w: status=%s", ErrOrderNotCancelable, order.Status)
	}

	if !status.CanTransition(order.Status, status.Canceling) {
		return nil, fmt.Errorf("%w: status=%s", ErrOrderNotCancelable, order.Status)
	}

	const upd = `
UPDATE orders
SET status = $1, version = version + 1, updated_at = now()
WHERE id = $2 AND user_id = $3 AND status = ANY($4) AND version = $5`
	allowed := status.AllowedFromStatuses(status.Canceling)
	tag, err := tx.Exec(ctx, upd, status.Canceling, orderID, userID, allowed, order.Version)
	if err != nil {
		return nil, fmt.Errorf("update canceling: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: concurrent update", ErrOrderNotCancelable)
	}

	order.Status = status.Canceling
	order.Version++

	if err := insertCancelOutbox(ctx, tx, order, outboxTopic); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return order, nil
}

func getOrderForUpdateByUser(ctx context.Context, tx pgx.Tx, userID, orderID uint64) (*Order, error) {
	const q = `
SELECT id, user_id, client_order_id, symbol, side, order_type,
       price::text, freeze_price::text, freeze_slippage::text, frozen_amount::text,
       quantity::text, filled_quantity::text,
       status, version, created_at, updated_at
FROM orders
WHERE id = $1 AND user_id = $2
FOR UPDATE`

	row := tx.QueryRow(ctx, q, orderID, userID)
	order, err := scanOrder(row)
	if err != nil {
		return nil, err
	}
	if order == nil {
		return nil, fmt.Errorf("%w: %d", ErrOrderNotFound, orderID)
	}
	return order, nil
}

func insertCancelOutbox(ctx context.Context, tx pgx.Tx, order *Order, topic string) error {
	outboxID, err := allocateOutboxID(ctx, tx)
	if err != nil {
		return err
	}

	payload, err := outbox.BuildCancelOrderPayload(order.Symbol, order.ID, outboxID)
	if err != nil {
		return fmt.Errorf("build cancel payload: %w", err)
	}

	return insertOutboxRow(ctx, tx, outboxID, order.ID, outbox.EventTypeCancelOrder, payload, topic, order.Symbol)
}
