package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// lockFunds 从可用余额划入冻结（balance -= amount, frozen += amount）。
// 单条乐观 UPDATE：在 SQL 内判 balance >= amount，避免 SELECT FOR UPDATE 拉长持锁时间。
func lockFunds(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	if amount.IsNegative() {
		return fmt.Errorf("lock funds: negative amount")
	}

	amt := amount.String()
	const upd = `
UPDATE account_balances
SET balance = balance - $1::numeric,
    frozen = frozen + $1::numeric,
    version = version + 1,
    updated_at = now()
WHERE user_id = $2 AND asset = $3 AND balance >= $1::numeric`
	tag, err := tx.Exec(ctx, upd, amt, userID, asset)
	if err != nil {
		return fmt.Errorf("lock funds update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: user=%d asset=%s", ErrInsufficientBalance, userID, asset)
	}
	return nil
}

// releaseFunds 从冻结划回可用（frozen -= amount, balance += amount）。
// 实际释放 min(frozen, amount)；frozen 已为 0 时幂等 no-op。
func releaseFunds(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	if amount.IsNegative() {
		return fmt.Errorf("release funds: negative amount")
	}

	amt := amount.String()
	const upd = `
UPDATE account_balances
SET balance = balance + LEAST(frozen, $1::numeric),
    frozen = frozen - LEAST(frozen, $1::numeric),
    version = version + 1,
    updated_at = now()
WHERE user_id = $2 AND asset = $3 AND frozen > 0`
	tag, err := tx.Exec(ctx, upd, amt, userID, asset)
	if err != nil {
		return fmt.Errorf("release funds update: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}

	// 未更新：无账户行，或 frozen 已为 0（撤单/成交重复释放等幂等场景）
	const checkQ = `SELECT frozen::text FROM account_balances WHERE user_id = $1 AND asset = $2`
	var frozenStr string
	err = tx.QueryRow(ctx, checkQ, userID, asset).Scan(&frozenStr)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("release funds: no balance row user=%d asset=%s", userID, asset)
	}
	if err != nil {
		return fmt.Errorf("release funds check: %w", err)
	}
	frozen, err := decimal.NewFromString(frozenStr)
	if err != nil {
		return err
	}
	if frozen.IsZero() {
		return nil
	}
	return fmt.Errorf("release funds update: no rows affected user=%d asset=%s", userID, asset)
}

// consumeFrozen 从冻结中扣减（成交占用，不再回到可用）。
func consumeFrozen(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	if amount.IsNegative() {
		return fmt.Errorf("consume frozen: negative amount")
	}

	amt := amount.String()
	const upd = `
UPDATE account_balances
SET frozen = frozen - $1::numeric,
    version = version + 1,
    updated_at = now()
WHERE user_id = $2 AND asset = $3 AND frozen >= $1::numeric`
	tag, err := tx.Exec(ctx, upd, amt, userID, asset)
	if err != nil {
		return fmt.Errorf("consume frozen update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: user=%d asset=%s need=%s",
			ErrInsufficientBalance, userID, asset, amt)
	}
	return nil
}

// creditBalance 增加可用余额（成交所得）。
func creditBalance(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	return adjustBalance(ctx, tx, userID, asset, amount)
}
