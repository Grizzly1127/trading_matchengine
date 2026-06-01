package repository

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Trade 成交记录（只读查询）。
type Trade struct {
	TradeID      uint64
	Symbol       string
	Price        string
	Quantity     string
	MakerOrderID uint64
	TakerOrderID uint64
	CreatedAt    time.Time
	UserOrderID  uint64
	Side         int16
	IsMaker      bool
}

// ListTradesFilter 成交列表查询条件。
type ListTradesFilter struct {
	UserID        uint64
	Symbol        string
	OrderID       uint64
	CreatedAtFrom *time.Time
	CreatedAtTo   *time.Time
	Page          int
	PageSize      int
}

// ListTrades 查询用户相关成交（maker 或 taker）。
func (r *Repository) ListTrades(ctx context.Context, f ListTradesFilter) ([]Trade, error) {
	if f.UserID == 0 {
		return nil, fmt.Errorf("list trades: user_id required")
	}
	page := max(f.Page, 1)
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
SELECT t.trade_id, t.symbol, t.price::text, t.quantity::text,
       t.maker_order_id, t.taker_order_id, t.created_at,
       uo.id, uo.side, (uo.id = t.maker_order_id) AS is_maker
FROM trades t
INNER JOIN LATERAL (
  SELECT o.id, o.side
  FROM orders o
  WHERE o.user_id = $1
    AND (o.id = t.maker_order_id OR o.id = t.taker_order_id)
  ORDER BY (o.id = t.maker_order_id) DESC
  LIMIT 1
) uo ON true
WHERE true`)
	args := []any{f.UserID}
	n := 1

	if strings.TrimSpace(f.Symbol) != "" {
		n++
		fmt.Fprintf(&b, " AND t.symbol = $%d", n)
		args = append(args, strings.TrimSpace(f.Symbol))
	}
	if f.OrderID != 0 {
		n++
		fmt.Fprintf(&b, " AND (t.maker_order_id = $%d OR t.taker_order_id = $%d)", n, n)
		args = append(args, f.OrderID)
	}
	if f.CreatedAtFrom != nil {
		n++
		fmt.Fprintf(&b, " AND t.created_at >= $%d", n)
		args = append(args, *f.CreatedAtFrom)
	}
	if f.CreatedAtTo != nil {
		n++
		fmt.Fprintf(&b, " AND t.created_at <= $%d", n)
		args = append(args, *f.CreatedAtTo)
	}
	n++
	fmt.Fprintf(&b, " ORDER BY t.trade_id DESC LIMIT $%d", n)
	args = append(args, pageSize)
	n++
	fmt.Fprintf(&b, " OFFSET $%d", n)
	args = append(args, offset)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list trades: %w", err)
	}
	defer rows.Close()

	var out []Trade
	for rows.Next() {
		var t Trade
		if err := rows.Scan(&t.TradeID, &t.Symbol, &t.Price, &t.Quantity,
			&t.MakerOrderID, &t.TakerOrderID, &t.CreatedAt,
			&t.UserOrderID, &t.Side, &t.IsMaker); err != nil {
			return nil, fmt.Errorf("list trades scan: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list trades rows: %w", err)
	}
	return out, nil
}
