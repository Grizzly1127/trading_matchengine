package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const outboxIDSeq = `order_outbox_id_seq`

func allocateOutboxID(ctx context.Context, tx pgx.Tx) (uint64, error) {
	var id uint64
	if err := tx.QueryRow(ctx, `SELECT nextval($1)`, outboxIDSeq).Scan(&id); err != nil {
		return 0, fmt.Errorf("allocate outbox id: %w", err)
	}
	return id, nil
}

func insertOutboxRow(
	ctx context.Context,
	tx pgx.Tx,
	id, aggregateID uint64,
	eventType string,
	payload []byte,
	topic, partitionKey string,
) error {
	const q = `
INSERT INTO order_outbox (id, aggregate_id, event_type, payload, topic, partition_key)
VALUES ($1, $2, $3, $4, $5, $6)`
	if _, err := tx.Exec(ctx, q, id, aggregateID, eventType, payload, topic, partitionKey); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}
