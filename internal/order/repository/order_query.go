package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetOrderByID 按 order_id 查询订单。
func (r *Repository) GetOrderByID(ctx context.Context, orderID uint64) (*Order, error) {
	return getOrderByID(ctx, r.pool, orderID)
}

// GetOrderByUser 按 user_id + order_id 查询订单。
func (r *Repository) GetOrderByUser(ctx context.Context, userID, orderID uint64) (*Order, error) {
	const q = `
SELECT id, user_id, client_order_id, symbol, side, order_type,
       price::text, freeze_price::text, freeze_slippage::text, frozen_amount::text,
       quantity::text, filled_quantity::text,
       status, version, created_at, updated_at
FROM orders
WHERE id = $1 AND user_id = $2`

	row := r.pool.QueryRow(ctx, q, orderID, userID)
	order, err := scanOrder(row)
	if err != nil {
		return nil, err
	}
	if order == nil {
		return nil, fmt.Errorf("%w: %d", ErrOrderNotFound, orderID)
	}
	return order, nil
}

func getOrderByID(ctx context.Context, db queryRower, orderID uint64) (*Order, error) {
	const q = `
SELECT id, user_id, client_order_id, symbol, side, order_type,
       price::text, freeze_price::text, freeze_slippage::text, frozen_amount::text,
       quantity::text, filled_quantity::text,
       status, version, created_at, updated_at
FROM orders
WHERE id = $1`

	row := db.QueryRow(ctx, q, orderID)
	order, err := scanOrder(row)
	if err != nil {
		return nil, err
	}
	if order == nil {
		return nil, fmt.Errorf("%w: %d", ErrOrderNotFound, orderID)
	}
	return order, nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	_ queryRower = (*pgxpool.Pool)(nil)
)

// ErrOrderNotFound 表示订单不存在。
var ErrOrderNotFound = errors.New("order not found")

// ErrInsufficientBalance 表示余额不足。
var ErrInsufficientBalance = errors.New("insufficient balance")
