CREATE TABLE IF NOT EXISTS trades (
    trade_id         BIGINT PRIMARY KEY,
    symbol           VARCHAR(32) NOT NULL,
    price            NUMERIC(36, 18) NOT NULL,
    quantity         NUMERIC(36, 18) NOT NULL,
    maker_order_id   BIGINT NOT NULL,
    taker_order_id   BIGINT NOT NULL,
    wal_seq          BIGINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol);
CREATE INDEX IF NOT EXISTS idx_trades_maker_order_id ON trades (maker_order_id);
CREATE INDEX IF NOT EXISTS idx_trades_taker_order_id ON trades (taker_order_id);
