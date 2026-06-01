package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
)

// MatchEventApply 应用 match.events 的输入。
type MatchEventApply struct {
	OrderID        uint64
	Symbol         string
	EventType      int16
	WalSeq         uint64
	FilledQuantity *string
}

// ApplyMatchEvent 幂等更新订单状态（乐观锁 CAS）。applied 表示本条 match 事件首次生效。
func (r *Repository) ApplyMatchEvent(ctx context.Context, in MatchEventApply) (applied bool, err error) {
	target, err := status.TargetStatus(status.MatchEventType(in.EventType))
	if err != nil {
		return false, fmt.Errorf("apply match event: %w", err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	inserted, err := insertProcessedMatchEvent(ctx, tx, in.OrderID, in.WalSeq, in.EventType)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}

	order, err := getOrderForUpdate(ctx, tx, in.OrderID)
	if err != nil {
		return false, err
	}
	if order.Symbol != in.Symbol {
		return false, fmt.Errorf("apply match event: symbol mismatch order=%q event=%q", order.Symbol, in.Symbol)
	}

	if order.Status == target {
		if err := updateFilledQuantity(ctx, tx, order.ID, order.Version, in.FilledQuantity); err != nil {
			return false, err
		}
		// 仅撤单在 match 阶段释放剩余冻结；成交释放等 trade.events 结算后再做（避免 filled_quantity 未更新误释放全部冻结）。
		if target == status.Canceled {
			current, err := getOrderForUpdate(ctx, tx, order.ID)
			if err != nil {
				return false, err
			}
			if err := r.releaseOrderRemainingFreeze(ctx, tx, current); err != nil {
				return false, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}
	if status.IsTerminal(order.Status) {
		// 已 FILLED 等终态时忽略迟到的 ORDER_CANCELED（Matching 撤单 noop 仍会发事件）。
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}
	// 成交优先于撤单：CANCELING 可迁移至 ACCEPTED/PARTIAL/FILLED（见 status.AllowedFromStatuses）。
	if !status.CanTransition(order.Status, target) {
		return false, fmt.Errorf("apply match event: invalid transition %s -> %s", order.Status, target)
	}

	if err := transitionOrderStatus(ctx, tx, order, target, in.FilledQuantity); err != nil {
		if errors.Is(err, errCASConflict) {
			if err := tx.Commit(ctx); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	if target == status.Canceled {
		current, err := getOrderForUpdate(ctx, tx, order.ID)
		if err != nil {
			return false, err
		}
		if err := r.releaseOrderRemainingFreeze(ctx, tx, current); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Repository) releaseOrderRemainingFreeze(ctx context.Context, tx pgx.Tx, o *Order) error {
	spec, err := RemainingFreeze(o)
	if err != nil {
		return fmt.Errorf("release freeze: %w", err)
	}
	if spec.Amount.IsZero() {
		return nil
	}
	amount := r.roundDown(spec.Asset, spec.Amount)
	return releaseFunds(ctx, tx, o.UserID, spec.Asset, amount)
}

func insertProcessedMatchEvent(ctx context.Context, tx pgx.Tx, orderID uint64, walSeq uint64, eventType int16) (bool, error) {
	const q = `
INSERT INTO processed_match_events (order_id, wal_seq, event_type)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING`

	tag, err := tx.Exec(ctx, q, orderID, walSeq, eventType)
	if err != nil {
		return false, fmt.Errorf("insert processed match event: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func getOrderForUpdate(ctx context.Context, tx pgx.Tx, orderID uint64) (*Order, error) {
	const q = `
SELECT id, user_id, client_order_id, symbol, side, order_type,
       price::text, freeze_price::text, freeze_slippage::text, frozen_amount::text,
       quantity::text, filled_quantity::text,
       status, version, created_at, updated_at
FROM orders
WHERE id = $1
FOR UPDATE`

	row := tx.QueryRow(ctx, q, orderID)
	order, err := scanOrder(row)
	if err != nil {
		return nil, err
	}
	if order == nil {
		return nil, fmt.Errorf("%w: %d", ErrOrderNotFound, orderID)
	}
	return order, nil
}

var errCASConflict = errors.New("order status cas conflict")

func transitionOrderStatus(ctx context.Context, tx pgx.Tx, order *Order, target string, filled *string) error {
	allowed := status.AllowedFromStatuses(target)
	const q = `
UPDATE orders
SET status = $1,
    filled_quantity = COALESCE($2::numeric, filled_quantity),
    version = version + 1,
    updated_at = now()
WHERE id = $3 AND status = ANY($4) AND version = $5`

	tag, err := tx.Exec(ctx, q, target, filled, order.ID, allowed, order.Version)
	if err != nil {
		return fmt.Errorf("update order status: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	// 并发下可能已被其他事件更新到目标态，再读一次判断。
	current, err := getOrderForUpdate(ctx, tx, order.ID)
	if err != nil {
		return err
	}
	if current.Status == target || status.IsTerminal(current.Status) {
		return updateFilledQuantity(ctx, tx, current.ID, current.Version, filled)
	}
	return errCASConflict
}

func updateFilledQuantity(ctx context.Context, tx pgx.Tx, orderID uint64, version int32, filled *string) error {
	if filled == nil {
		return nil
	}
	const q = `
UPDATE orders
SET filled_quantity = $1::numeric,
    version = version + 1,
    updated_at = now()
WHERE id = $2 AND version = $3`
	_, err := tx.Exec(ctx, q, *filled, orderID, version)
	if err != nil {
		return fmt.Errorf("update filled quantity: %w", err)
	}
	return nil
}

// FilledQuantityFromRemaining 根据 quantity 与 remaining 计算已成交量。
func FilledQuantityFromRemaining(quantity, remaining string) (string, error) {
	qty, err := decimal.NewFromString(quantity)
	if err != nil {
		return "", fmt.Errorf("quantity: %w", err)
	}
	rem, err := decimal.NewFromString(remaining)
	if err != nil {
		return "", fmt.Errorf("remaining: %w", err)
	}
	filled := qty.Sub(rem)
	if filled.IsNegative() {
		return "", fmt.Errorf("filled quantity negative")
	}
	return filled.String(), nil
}
