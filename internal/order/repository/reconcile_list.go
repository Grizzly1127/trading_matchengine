package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
)

// ReconcileOrderRow 启动对账用订单摘要。
type ReconcileOrderRow struct {
	OrderID uint64
	Status  string
}

// ListReconcileOrders 返回 symbol 下 PENDING/PARTIAL 订单（§5.6）。
func (r *Repository) ListReconcileOrders(ctx context.Context, symbol string) ([]ReconcileOrderRow, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return nil, fmt.Errorf("list reconcile orders: symbol required")
	}
	rows, err := r.pool.Query(ctx, `
SELECT id, status
FROM orders
WHERE symbol = $1 AND status IN ($2, $3)
ORDER BY id`, symbol, status.Pending, status.Partial)
	if err != nil {
		return nil, fmt.Errorf("list reconcile orders: %w", err)
	}
	defer rows.Close()

	var out []ReconcileOrderRow
	for rows.Next() {
		var row ReconcileOrderRow
		if err := rows.Scan(&row.OrderID, &row.Status); err != nil {
			return nil, fmt.Errorf("list reconcile orders scan: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list reconcile orders rows: %w", err)
	}
	return out, nil
}

// GetOrderStatuses 批量查询 symbol 下订单状态。
func (r *Repository) GetOrderStatuses(ctx context.Context, symbol string, orderIDs []uint64) (map[uint64]string, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return nil, fmt.Errorf("get order statuses: symbol required")
	}
	if len(orderIDs) == 0 {
		return map[uint64]string{}, nil
	}
	rows, err := r.pool.Query(ctx, `
SELECT id, status
FROM orders
WHERE symbol = $1 AND id = ANY($2)`, symbol, orderIDs)
	if err != nil {
		return nil, fmt.Errorf("get order statuses: %w", err)
	}
	defer rows.Close()

	out := make(map[uint64]string, len(orderIDs))
	for rows.Next() {
		var id uint64
		var st string
		if err := rows.Scan(&id, &st); err != nil {
			return nil, fmt.Errorf("get order statuses scan: %w", err)
		}
		out[id] = st
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get order statuses rows: %w", err)
	}
	return out, nil
}
