CREATE TABLE IF NOT EXISTS account_balances (
    user_id        BIGINT NOT NULL,
    asset          VARCHAR(16) NOT NULL,
    balance        NUMERIC(36, 18) NOT NULL DEFAULT 0,
    frozen         NUMERIC(36, 18) NOT NULL DEFAULT 0,
    version        INT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, asset)
);

CREATE INDEX IF NOT EXISTS idx_account_balances_user_id ON account_balances (user_id);
