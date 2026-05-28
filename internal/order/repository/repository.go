package repository

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Order 持久化订单记录。
type Order struct {
	ID             uint64
	UserID         uint64
	ClientOrderID  string
	Symbol         string
	Side           int16
	OrderType      int16
	Price          *string
	FreezePrice    *string
	FreezeSlippage *string
	FrozenAmount   *string
	Quantity       string
	FilledQuantity string
	Status         string
	Version        int32
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Repository 封装 PostgreSQL 订单读写。
type Repository struct {
	pool *pgxpool.Pool
}

// New 创建 Repository。
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// NewPool 创建连接池。
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// OutboxEntry 是 outbox.Entry 的别名。
type OutboxEntry = outbox.Entry

// MigrateUp 执行内嵌迁移（按文件名顺序）。
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := pool.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}

// FindByClientOrderID 按幂等键查已有订单。
func (r *Repository) FindByClientOrderID(ctx context.Context, userID uint64, clientOrderID string) (*Order, error) {
	const q = `
SELECT o.id, o.user_id, o.client_order_id, o.symbol, o.side, o.order_type,
       o.price::text, o.freeze_price::text, o.freeze_slippage::text, o.frozen_amount::text,
       o.quantity::text, o.filled_quantity::text,
       o.status, o.version, o.created_at, o.updated_at
FROM client_order_idempotency i
JOIN orders o ON o.id = i.order_id
WHERE i.user_id = $1 AND i.client_order_id = $2`

	row := r.pool.QueryRow(ctx, q, userID, clientOrderID)
	return scanOrder(row)
}

// InsertPending 在同一事务内写入 orders、幂等表与 order_outbox。
func (r *Repository) InsertPending(ctx context.Context, in InsertPendingInput) (*Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const insertOrder = `
INSERT INTO orders (user_id, client_order_id, symbol, side, order_type, price, freeze_price, freeze_slippage, frozen_amount, quantity, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'PENDING')
RETURNING id, user_id, client_order_id, symbol, side, order_type,
          price::text, freeze_price::text, freeze_slippage::text, frozen_amount::text,
          quantity::text, filled_quantity::text,
          status, version, created_at, updated_at`

	row := tx.QueryRow(ctx, insertOrder,
		in.UserID,
		in.ClientOrderID,
		in.Symbol,
		in.Side,
		in.OrderType,
		in.Price,
		in.FreezePrice,
		in.FreezeSlippage,
		in.FrozenAmount,
		in.Quantity,
	)
	order, err := scanOrder(row)
	if err != nil {
		return nil, err
	}

	const insertIdem = `
INSERT INTO client_order_idempotency (user_id, client_order_id, order_id)
VALUES ($1, $2, $3)`
	if _, err := tx.Exec(ctx, insertIdem, in.UserID, in.ClientOrderID, order.ID); err != nil {
		return nil, fmt.Errorf("insert idempotency: %w", err)
	}

	freeze, err := ComputeFreeze(in.Side, in.Symbol, in.Price, in.Quantity, in.FrozenAmount)
	if err != nil {
		return nil, fmt.Errorf("compute freeze: %w", err)
	}
	if err := lockFunds(ctx, tx, in.UserID, freeze.Asset, freeze.Amount); err != nil {
		return nil, err
	}

	if err := insertNewOrderOutbox(ctx, tx, order, in.OutboxTopic); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return order, nil
}

func insertNewOrderOutbox(ctx context.Context, tx pgx.Tx, order *Order, topic string) error {
	const insertOutbox = `
INSERT INTO order_outbox (aggregate_id, event_type, payload, topic, partition_key)
VALUES ($1, $2, $3, $4, $5)
RETURNING id`

	var outboxID uint64
	if err := tx.QueryRow(ctx, insertOutbox,
		order.ID,
		outbox.EventTypeNewOrder,
		[]byte{},
		topic,
		order.Symbol,
	).Scan(&outboxID); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	payload, err := outbox.BuildNewOrderPayload(orderSnapshot(order), outboxID)
	if err != nil {
		return fmt.Errorf("build outbox payload: %w", err)
	}

	const updatePayload = `UPDATE order_outbox SET payload = $1 WHERE id = $2`
	if _, err := tx.Exec(ctx, updatePayload, payload, outboxID); err != nil {
		return fmt.Errorf("update outbox payload: %w", err)
	}
	return nil
}

func orderSnapshot(o *Order) outbox.OrderSnapshot {
	return outbox.OrderSnapshot{
		ID:            o.ID,
		ClientOrderID: o.ClientOrderID,
		Symbol:        o.Symbol,
		Side:          o.Side,
		OrderType:     o.OrderType,
		Price:         o.Price,
		Quantity:      o.Quantity,
		CreatedAt:     o.CreatedAt,
		UpdatedAt:     o.UpdatedAt,
	}
}

// FetchUnpublished 按 created_at 升序读取待投递 Outbox。
func (r *Repository) FetchUnpublished(ctx context.Context, limit int) ([]outbox.Entry, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
SELECT id, aggregate_id, event_type, payload, topic, partition_key, retry_count
FROM order_outbox
WHERE published_at IS NULL
ORDER BY created_at ASC
LIMIT $1`

	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unpublished outbox: %w", err)
	}
	defer rows.Close()

	var out []outbox.Entry
	for rows.Next() {
		var e outbox.Entry
		if err := rows.Scan(
			&e.ID,
			&e.AggregateID,
			&e.EventType,
			&e.Payload,
			&e.Topic,
			&e.PartitionKey,
			&e.RetryCount,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch unpublished outbox: %w", err)
	}
	return out, nil
}

// MarkPublished 标记 Outbox 已投递。
func (r *Repository) MarkPublished(ctx context.Context, id uint64) error {
	const q = `UPDATE order_outbox SET published_at = now() WHERE id = $1 AND published_at IS NULL`
	tag, err := r.pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark outbox published: id %d not found or already published", id)
	}
	return nil
}

// IncrementRetry 投递失败时递增重试计数。
func (r *Repository) IncrementRetry(ctx context.Context, id uint64) error {
	const q = `UPDATE order_outbox SET retry_count = retry_count + 1 WHERE id = $1`
	if _, err := r.pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("increment outbox retry: %w", err)
	}
	return nil
}

// GetOrderStatus 查询订单当前状态（Relay 投递前校验）。
func (r *Repository) GetOrderStatus(ctx context.Context, orderID uint64) (string, error) {
	const q = `SELECT status FROM orders WHERE id = $1`
	var status string
	if err := r.pool.QueryRow(ctx, q, orderID).Scan(&status); err != nil {
		if err == pgx.ErrNoRows {
			return "", fmt.Errorf("order %d not found", orderID)
		}
		return "", fmt.Errorf("get order status: %w", err)
	}
	return status, nil
}

// InsertPendingInput 新订单写入参数。
type InsertPendingInput struct {
	UserID         uint64
	ClientOrderID  string
	Symbol         string
	Side           int16
	OrderType      int16
	Price          *string
	FreezePrice    *string
	FreezeSlippage *string
	FrozenAmount   *string
	Quantity       string
	OutboxTopic    string
}

func scanOrder(row pgx.Row) (*Order, error) {
	var o Order
	var price *string
	var freezePrice *string
	var freezeSlippage *string
	var frozenAmount *string
	if err := row.Scan(
		&o.ID,
		&o.UserID,
		&o.ClientOrderID,
		&o.Symbol,
		&o.Side,
		&o.OrderType,
		&price,
		&freezePrice,
		&freezeSlippage,
		&frozenAmount,
		&o.Quantity,
		&o.FilledQuantity,
		&o.Status,
		&o.Version,
		&o.CreatedAt,
		&o.UpdatedAt,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("scan order: %w", err)
	}
	if price != nil && strings.TrimSpace(*price) != "" {
		o.Price = price
	}
	if freezePrice != nil && strings.TrimSpace(*freezePrice) != "" {
		o.FreezePrice = freezePrice
	}
	if freezeSlippage != nil && strings.TrimSpace(*freezeSlippage) != "" {
		o.FreezeSlippage = freezeSlippage
	}
	if frozenAmount != nil && strings.TrimSpace(*frozenAmount) != "" {
		o.FrozenAmount = frozenAmount
	}
	return &o, nil
}
