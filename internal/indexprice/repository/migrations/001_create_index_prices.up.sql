CREATE TABLE IF NOT EXISTS index_prices (
    id         BIGSERIAL PRIMARY KEY,
    symbol     TEXT NOT NULL,
    price      NUMERIC NOT NULL,
    ts         TIMESTAMPTZ NOT NULL,
    sources    TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_index_prices_symbol_ts
    ON index_prices (symbol, ts DESC);
