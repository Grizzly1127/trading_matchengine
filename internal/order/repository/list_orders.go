package repository

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ListOrdersFilter 订单列表查询条件（空值/0 表示不筛选）。
type ListOrdersFilter struct {
	UserID        uint64
	Symbol        string
	Side          int16 // 0 表示不限
	OrderType     int16 // 0 表示不限
	Status        string
	CreatedAtFrom *time.Time
	CreatedAtTo   *time.Time
	Page          int
	PageSize      int
}

// ListOrders 按条件分页查询订单（按 id 降序）。
func (r *Repository) ListOrders(ctx context.Context, f ListOrdersFilter) ([]Order, error) {
	if f.UserID == 0 {
		return nil, fmt.Errorf("list orders: user_id required")
	}
	page := f.Page
	if page < 1 {
		page = 1
	}
	pageSize := f.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	offset := (page - 1) * pageSize

	var b strings.Builder
	b.WriteString(`
SELECT id, user_id, client_order_id, symbol, side, order_type,
       price::text, quantity::text, filled_quantity::text,
       status, version, created_at, updated_at
FROM orders
WHERE user_id = $1`)

	args := []any{f.UserID}
	n := 1

	if strings.TrimSpace(f.Symbol) != "" {
		n++
		fmt.Fprintf(&b, " AND symbol = $%d", n)
		args = append(args, strings.TrimSpace(f.Symbol))
	}
	if f.Side != 0 {
		n++
		fmt.Fprintf(&b, " AND side = $%d", n)
		args = append(args, f.Side)
	}
	if f.OrderType != 0 {
		n++
		fmt.Fprintf(&b, " AND order_type = $%d", n)
		args = append(args, f.OrderType)
	}
	if strings.TrimSpace(f.Status) != "" {
		n++
		fmt.Fprintf(&b, " AND status = $%d", n)
		args = append(args, strings.TrimSpace(strings.ToUpper(f.Status)))
	}
	if f.CreatedAtFrom != nil {
		n++
		fmt.Fprintf(&b, " AND created_at >= $%d", n)
		args = append(args, *f.CreatedAtFrom)
	}
	if f.CreatedAtTo != nil {
		n++
		fmt.Fprintf(&b, " AND created_at <= $%d", n)
		args = append(args, *f.CreatedAtTo)
	}

	n++
	fmt.Fprintf(&b, " ORDER BY id DESC LIMIT $%d", n)
	args = append(args, pageSize)
	n++
	fmt.Fprintf(&b, " OFFSET $%d", n)
	args = append(args, offset)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	var out []Order
	for rows.Next() {
		order, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		if order != nil {
			out = append(out, *order)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list orders rows: %w", err)
	}
	return out, nil
}
