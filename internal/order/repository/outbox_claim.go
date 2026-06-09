package repository

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const claimUnpublishedSQL = `
SELECT id, aggregate_id, event_type, payload, topic, partition_key, retry_count
FROM order_outbox
WHERE published_at IS NULL
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED`

type unpublishedClaim struct {
	tx      pgx.Tx
	entries []outbox.Entry
	done    bool
}

func (c *unpublishedClaim) Entries() []outbox.Entry {
	return c.entries
}

func (c *unpublishedClaim) MarkPublishedBatch(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 || c.done {
		return nil
	}
	const q = `UPDATE order_outbox SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`
	if _, err := c.tx.Exec(ctx, q, ids); err != nil {
		return fmt.Errorf("mark outbox published batch in claim: %w", err)
	}
	return nil
}

func (c *unpublishedClaim) Commit(ctx context.Context) error {
	if c.done {
		return nil
	}
	c.done = true
	if err := c.tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox claim: %w", err)
	}
	return nil
}

func (c *unpublishedClaim) Rollback(ctx context.Context) error {
	if c.done {
		return nil
	}
	c.done = true
	if err := c.tx.Rollback(ctx); err != nil {
		return fmt.Errorf("rollback outbox claim: %w", err)
	}
	return nil
}

// ClaimUnpublished 在事务内 SKIP LOCKED 领取一批待投递 Outbox；成功路径需 Commit。
func (r *Repository) ClaimUnpublished(ctx context.Context, limit int) (outbox.ClaimHandle, error) {
	return r.claimUnpublished(ctx, r.pool, limit)
}

func (r *Repository) claimUnpublished(ctx context.Context, pool *pgxpool.Pool, limit int) (outbox.ClaimHandle, error) {
	if limit <= 0 {
		limit = 50
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}

	rows, err := tx.Query(ctx, claimUnpublishedSQL, limit)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("claim unpublished outbox: %w", err)
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
			_ = tx.Rollback(ctx)
			return nil, fmt.Errorf("scan outbox claim: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("claim unpublished outbox: %w", err)
	}
	if len(out) == 0 {
		_ = tx.Rollback(ctx)
		return outbox.EmptyClaim(), nil
	}
	return &unpublishedClaim{tx: tx, entries: out}, nil
}
