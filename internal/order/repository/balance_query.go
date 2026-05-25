package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// AccountBalance 用户单资产余额快照。
type AccountBalance struct {
	UserID  uint64
	Asset   string
	Balance decimal.Decimal
	Frozen  decimal.Decimal
}

// Available 可用余额 = balance - frozen。
func (b *AccountBalance) Available() decimal.Decimal {
	return b.Balance.Sub(b.Frozen)
}

// ErrBalanceNotFound 表示该用户资产账户不存在。
var ErrBalanceNotFound = errors.New("balance not found")

// GetBalance 查询用户指定资产余额。
func (r *Repository) GetBalance(ctx context.Context, userID uint64, asset string) (*AccountBalance, error) {
	const q = `
SELECT user_id, asset, balance::text, frozen::text
FROM account_balances
WHERE user_id = $1 AND asset = $2`

	row := r.pool.QueryRow(ctx, q, userID, asset)
	return scanAccountBalance(row)
}

// ListBalances 查询用户全部资产余额。
func (r *Repository) ListBalances(ctx context.Context, userID uint64) ([]AccountBalance, error) {
	const q = `
SELECT user_id, asset, balance::text, frozen::text
FROM account_balances
WHERE user_id = $1
ORDER BY asset`

	rows, err := r.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, fmt.Errorf("list balances: %w", err)
	}
	defer rows.Close()

	var out []AccountBalance
	for rows.Next() {
		var userID uint64
		var asset, balanceStr, frozenStr string
		if err := rows.Scan(&userID, &asset, &balanceStr, &frozenStr); err != nil {
			return nil, fmt.Errorf("scan balance row: %w", err)
		}
		bal, err := decimal.NewFromString(balanceStr)
		if err != nil {
			return nil, fmt.Errorf("parse balance: %w", err)
		}
		frozen, err := decimal.NewFromString(frozenStr)
		if err != nil {
			return nil, fmt.Errorf("parse frozen: %w", err)
		}
		out = append(out, AccountBalance{
			UserID:  userID,
			Asset:   asset,
			Balance: bal,
			Frozen:  frozen,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list balances rows: %w", err)
	}
	return out, nil
}

func scanAccountBalance(row pgx.Row) (*AccountBalance, error) {
	var userID uint64
	var asset, balanceStr, frozenStr string
	err := row.Scan(&userID, &asset, &balanceStr, &frozenStr)
	if err == pgx.ErrNoRows {
		return nil, ErrBalanceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan balance: %w", err)
	}
	balance, err := decimal.NewFromString(balanceStr)
	if err != nil {
		return nil, fmt.Errorf("parse balance: %w", err)
	}
	frozen, err := decimal.NewFromString(frozenStr)
	if err != nil {
		return nil, fmt.Errorf("parse frozen: %w", err)
	}
	return &AccountBalance{
		UserID:  userID,
		Asset:   asset,
		Balance: balance,
		Frozen:  frozen,
	}, nil
}
