CREATE TABLE IF NOT EXISTS processed_match_events (
    order_id     BIGINT NOT NULL,
    wal_seq      BIGINT NOT NULL,
    event_type   SMALLINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, wal_seq, event_type)
);
