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
	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
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
	pool      *pgxpool.Pool
	relayPool *pgxpool.Pool
	assets    *symbolrules.AssetRegistry
}

// New 创建 Repository。
func New(pool *pgxpool.Pool, assets *symbolrules.AssetRegistry) *Repository {
	if assets == nil {
		assets, _ = symbolrules.DefaultAssetRegistry()
	}
	return &Repository{pool: pool, assets: assets}
}

func (r *Repository) roundDown(asset string, amount decimal.Decimal) decimal.Decimal {
	if r == nil || r.assets == nil {
		reg, _ := symbolrules.DefaultAssetRegistry()
		return reg.RoundDown(asset, amount)
	}
	return r.assets.RoundDown(asset, amount)
}

func (r *Repository) roundUp(asset string, amount decimal.Decimal) decimal.Decimal {
	if r == nil || r.assets == nil {
		reg, _ := symbolrules.DefaultAssetRegistry()
		return reg.RoundUp(asset, amount)
	}
	return r.assets.RoundUp(asset, amount)
}

// NewPool 创建连接池；maxConns≤0 时使用 pgx 默认上限。
func NewPool(ctx context.Context, databaseURL string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
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
	if err := lockFunds(ctx, tx, in.UserID, freeze.Asset, r.roundUp(freeze.Asset, freeze.Amount)); err != nil {
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
	outboxID, err := allocateOutboxID(ctx, tx)
	if err != nil {
		return err
	}

	payload, err := outbox.BuildNewOrderPayload(orderSnapshot(order), outboxID)
	if err != nil {
		return fmt.Errorf("build outbox payload: %w", err)
	}

	return insertOutboxRow(ctx, tx, outboxID, order.ID, outbox.EventTypeNewOrder, payload, topic, order.Symbol)
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
	return r.markPublished(ctx, r.pool, id)
}

func (r *Repository) markPublished(ctx context.Context, pool *pgxpool.Pool, id uint64) error {
	const q = `UPDATE order_outbox SET published_at = now() WHERE id = $1 AND published_at IS NULL`
	tag, err := pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark outbox published: id %d not found or already published", id)
	}
	return nil
}

// MarkPublishedBatch 批量标记 Outbox 已投递。
func (r *Repository) MarkPublishedBatch(ctx context.Context, ids []uint64) error {
	return r.markPublishedBatch(ctx, r.pool, ids)
}

func (r *Repository) markPublishedBatch(ctx context.Context, pool *pgxpool.Pool, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	const q = `UPDATE order_outbox SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`
	if _, err := pool.Exec(ctx, q, ids); err != nil {
		return fmt.Errorf("mark outbox published batch: %w", err)
	}
	return nil
}

// IncrementRetry 投递失败时递增重试计数。
func (r *Repository) IncrementRetry(ctx context.Context, id uint64) error {
	return r.incrementRetry(ctx, r.pool, id)
}

func (r *Repository) incrementRetry(ctx context.Context, pool *pgxpool.Pool, id uint64) error {
	const q = `UPDATE order_outbox SET retry_count = retry_count + 1 WHERE id = $1`
	if _, err := pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("increment outbox retry: %w", err)
	}
	return nil
}

// GetOrderStatus 查询订单当前状态（Relay 投递前校验）。
func (r *Repository) GetOrderStatus(ctx context.Context, orderID uint64) (string, error) {
	statuses, err := r.GetOrderStatusesBatch(ctx, []uint64{orderID})
	if err != nil {
		return "", err
	}
	status, ok := statuses[orderID]
	if !ok {
		return "", fmt.Errorf("order %d not found", orderID)
	}
	return status, nil
}

// GetOrderStatusesBatch 批量查询订单状态（Relay 投递前校验）。
func (r *Repository) GetOrderStatusesBatch(ctx context.Context, orderIDs []uint64) (map[uint64]string, error) {
	return r.getOrderStatusesBatch(ctx, r.pool, orderIDs)
}

func (r *Repository) getOrderStatusesBatch(ctx context.Context, pool *pgxpool.Pool, orderIDs []uint64) (map[uint64]string, error) {
	if len(orderIDs) == 0 {
		return map[uint64]string{}, nil
	}
	const q = `SELECT id, status FROM orders WHERE id = ANY($1)`
	rows, err := pool.Query(ctx, q, orderIDs)
	if err != nil {
		return nil, fmt.Errorf("get order statuses batch: %w", err)
	}
	defer rows.Close()

	out := make(map[uint64]string, len(orderIDs))
	for rows.Next() {
		var id uint64
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			return nil, fmt.Errorf("scan order status: %w", err)
		}
		out[id] = status
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get order statuses batch: %w", err)
	}
	return out, nil
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
