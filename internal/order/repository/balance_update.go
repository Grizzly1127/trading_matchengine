package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// UpdateBalanceInput 调整可用余额（幂等键：user_id + business + business_id）。
type UpdateBalanceInput struct {
	UserID     uint64
	Asset      string
	Business   string
	BusinessID uint64
	Change     decimal.Decimal
}

// UpdateBalance 按幂等键调整 balance；重复请求返回当前余额且不重复加减。
func (r *Repository) UpdateBalance(ctx context.Context, in UpdateBalanceInput) (*AccountBalance, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const ins = `
INSERT INTO balance_adjust_idempotency (user_id, business, business_id, asset)
VALUES ($1, $2, $3, $4)
ON CONFLICT (user_id, business, business_id) DO NOTHING`

	tag, err := tx.Exec(ctx, ins, in.UserID, in.Business, in.BusinessID, in.Asset)
	if err != nil {
		return nil, fmt.Errorf("insert balance idempotency: %w", err)
	}
	if tag.RowsAffected() == 0 {
		bal, err := getBalanceInTx(ctx, tx, in.UserID, in.Asset)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit tx: %w", err)
		}
		return bal, nil
	}

	if err := adjustBalance(ctx, tx, in.UserID, in.Asset, in.Change); err != nil {
		return nil, err
	}

	bal, err := getBalanceInTx(ctx, tx, in.UserID, in.Asset)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return bal, nil
}

func getBalanceInTx(ctx context.Context, tx pgx.Tx, userID uint64, asset string) (*AccountBalance, error) {
	const q = `
SELECT user_id, asset, balance::text, frozen::text
FROM account_balances
WHERE user_id = $1 AND asset = $2`

	row := tx.QueryRow(ctx, q, userID, asset)
	return scanAccountBalance(row)
}
