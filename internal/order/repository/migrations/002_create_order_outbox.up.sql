CREATE TABLE IF NOT EXISTS order_outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  BIGINT NOT NULL,
    event_type    VARCHAR(32) NOT NULL,
    payload       BYTEA NOT NULL,
    topic         VARCHAR(64) NOT NULL,
    partition_key VARCHAR(32) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    retry_count   INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON order_outbox (created_at)
    WHERE published_at IS NULL;
