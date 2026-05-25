CREATE TABLE IF NOT EXISTS balance_adjust_idempotency (
    user_id      BIGINT NOT NULL,
    business     VARCHAR(32) NOT NULL,
    business_id  BIGINT NOT NULL,
    asset        VARCHAR(16) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, business, business_id)
);
