package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// lockFunds 从可用余额划入冻结（balance -= amount, frozen += amount）。
func lockFunds(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	if amount.IsNegative() {
		return fmt.Errorf("lock funds: negative amount")
	}

	const selectQ = `
SELECT balance, frozen, version FROM account_balances
WHERE user_id = $1 AND asset = $2
FOR UPDATE`

	var balanceStr, frozenStr string
	var version int32
	err := tx.QueryRow(ctx, selectQ, userID, asset).Scan(&balanceStr, &frozenStr, &version)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("%w: user=%d asset=%s", ErrInsufficientBalance, userID, asset)
	}
	if err != nil {
		return fmt.Errorf("lock funds select: %w", err)
	}

	balance, err := decimal.NewFromString(balanceStr)
	if err != nil {
		return err
	}
	frozen, err := decimal.NewFromString(frozenStr)
	if err != nil {
		return err
	}
	if balance.LessThan(amount) {
		return fmt.Errorf("%w: user=%d asset=%s", ErrInsufficientBalance, userID, asset)
	}

	const upd = `
UPDATE account_balances
SET balance = $1, frozen = $2, version = version + 1, updated_at = now()
WHERE user_id = $3 AND asset = $4 AND version = $5`
	_, err = tx.Exec(ctx, upd,
		balance.Sub(amount).String(),
		frozen.Add(amount).String(),
		userID, asset, version,
	)
	if err != nil {
		return fmt.Errorf("lock funds update: %w", err)
	}
	return nil
}

// releaseFunds 从冻结划回可用（frozen -= amount, balance += amount）。
func releaseFunds(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	if amount.IsNegative() {
		return fmt.Errorf("release funds: negative amount")
	}

	const selectQ = `
SELECT balance, frozen, version FROM account_balances
WHERE user_id = $1 AND asset = $2
FOR UPDATE`

	var balanceStr, frozenStr string
	var version int32
	err := tx.QueryRow(ctx, selectQ, userID, asset).Scan(&balanceStr, &frozenStr, &version)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("release funds: no balance row user=%d asset=%s", userID, asset)
	}
	if err != nil {
		return err
	}

	balance, _ := decimal.NewFromString(balanceStr)
	frozen, _ := decimal.NewFromString(frozenStr)
	if frozen.LessThan(amount) {
		amount = frozen
	}
	if amount.IsZero() {
		return nil
	}

	const upd = `
UPDATE account_balances
SET balance = $1, frozen = $2, version = version + 1, updated_at = now()
WHERE user_id = $3 AND asset = $4 AND version = $5`
	_, err = tx.Exec(ctx, upd,
		balance.Add(amount).String(),
		frozen.Sub(amount).String(),
		userID, asset, version,
	)
	if err != nil {
		return fmt.Errorf("release funds update: %w", err)
	}
	return nil
}

// consumeFrozen 从冻结中扣减（成交占用，不再回到可用）。
func consumeFrozen(ctx context.Context, tx pgx.Tx, userID uint64, asset string, amount decimal.Decimal) error {
	if amount.IsZero() {
		return nil
	}
	if amount.IsNegative() {
		return fmt.Errorf("consume frozen: negative amount")
	}

	const selectQ = `
SELECT frozen, version FROM account_balances
WHERE user_id = $1 AND asset = $2
FOR UPDATE`

	var frozenStr string
	var version int32
	err := tx.QueryRow(ctx, selectQ, userID, asset).Scan(&frozenStr, &version)
	if err != nil {
		return fmt.Errorf("consume frozen select: %w", err)
	}
	frozen, err := decimal.NewFromString(frozenStr)
	if err != nil {
		return err
	}
	if frozen.LessThan(amount) {
		return fmt.Errorf("%w: user=%d asset=%s frozen=%s need=%s",
			ErrInsufficientBalance, userID, asset, frozen.String(), amount.String())
	}

	const upd = `
UPDATE account_balances
SET frozen = $1, version = version + 1, updated_at = now()
WHERE user_id = $2 AND asset = $3 AND version = $4`
	_, err = tx.Exec(ctx, upd, frozen.Sub(amount).String(), userID, asset, version)
	if err != nil {
		return fmt.Errorf("consume frozen update: %w", err)
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
