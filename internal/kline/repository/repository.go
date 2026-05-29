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

	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// ClosedBar 已闭合 K 线持久化记录。
type ClosedBar struct {
	Symbol     string
	Interval   interval.Interval
	OpenTime   time.Time
	CloseTime  time.Time
	Open       string
	High       string
	Low        string
	Close      string
	Volume     string
}

// Repository PostgreSQL klines 表读写。
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

// MigrateUp 执行内嵌迁移。
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

// InsertClosed 幂等写入闭合 K 线。
func (r *Repository) InsertClosed(ctx context.Context, rec ClosedBar) error {
	const q = `
INSERT INTO klines (symbol, interval, open_time, close_time, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (symbol, interval, open_time) DO NOTHING`
	_, err := r.pool.Exec(ctx, q,
		rec.Symbol, string(rec.Interval), rec.OpenTime, rec.CloseTime,
		rec.Open, rec.High, rec.Low, rec.Close, rec.Volume,
	)
	if err != nil {
		return fmt.Errorf("insert closed kline: %w", err)
	}
	return nil
}

// ListQuery K 线历史查询参数。
type ListQuery struct {
	Symbol    string
	Interval  interval.Interval
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
}

// ListClosed 查询已闭合 K 线（按 open_time 降序）。
func (r *Repository) ListClosed(ctx context.Context, q ListQuery) ([]ClosedBar, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 500
	}
	if limit > 1500 {
		limit = 1500
	}

	var (
		args []interface{}
		b    strings.Builder
	)
	b.WriteString(`
SELECT symbol, interval, open_time, close_time,
       open::text, high::text, low::text, close::text, volume::text
FROM klines
WHERE symbol = $1 AND interval = $2`)
	args = append(args, q.Symbol, string(q.Interval))
	argN := 3
	if q.StartTime != nil {
		fmt.Fprintf(&b, " AND open_time >= $%d", argN)
		args = append(args, *q.StartTime)
		argN++
	}
	if q.EndTime != nil {
		fmt.Fprintf(&b, " AND open_time <= $%d", argN)
		args = append(args, *q.EndTime)
		argN++
	}
	fmt.Fprintf(&b, " ORDER BY open_time DESC LIMIT $%d", argN)
	args = append(args, limit)

	rows, err := r.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list klines: %w", err)
	}
	defer rows.Close()

	var out []ClosedBar
	for rows.Next() {
		var rec ClosedBar
		var iv string
		if err := rows.Scan(
			&rec.Symbol, &iv, &rec.OpenTime, &rec.CloseTime,
			&rec.Open, &rec.High, &rec.Low, &rec.Close, &rec.Volume,
		); err != nil {
			return nil, fmt.Errorf("scan kline: %w", err)
		}
		rec.Interval = interval.Interval(iv)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ClosedBarFromOHLCV 由内存 bar 构造持久化记录。
func ClosedBarFromOHLCV(symbol string, iv interval.Interval, b bar.OHLCV) ClosedBar {
	open := time.UnixMilli(b.OpenTimeMs).UTC()
	closeT := time.UnixMilli(iv.CloseTimeMs(b.OpenTimeMs)).UTC()
	return ClosedBar{
		Symbol:    symbol,
		Interval:  iv,
		OpenTime:  open,
		CloseTime: closeT,
		Open:      b.Open.String(),
		High:      b.High.String(),
		Low:       b.Low.String(),
		Close:     b.Close.String(),
		Volume:    b.Volume.String(),
	}
}

// ErrNoRows 别名。
var ErrNoRows = pgx.ErrNoRows
