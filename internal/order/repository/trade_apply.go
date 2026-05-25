package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/internal/order/symbol"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

// TradeEventApply 应用 trade.events 的输入。
type TradeEventApply struct {
	TradeID      uint64
	Symbol       string
	Price        string
	Quantity     string
	MakerOrderID uint64
	TakerOrderID uint64
	WalSeq       uint64
}

// ApplyTradeEvent 幂等写入成交并结算双方余额。
func (r *Repository) ApplyTradeEvent(ctx context.Context, in TradeEventApply) error {
	price, err := decimal.NewFromString(in.Price)
	if err != nil || !price.IsPositive() {
		return fmt.Errorf("apply trade event: invalid price %q", in.Price)
	}
	qty, err := decimal.NewFromString(in.Quantity)
	if err != nil || !qty.IsPositive() {
		return fmt.Errorf("apply trade event: invalid quantity %q", in.Quantity)
	}
	pair, err := symbol.ParsePair(in.Symbol)
	if err != nil {
		return fmt.Errorf("apply trade event: %w", err)
	}
	notional := price.Mul(qty)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	inserted, err := insertTrade(ctx, tx, in)
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}

	maker, err := getOrderForUpdate(ctx, tx, in.MakerOrderID)
	if err != nil {
		return err
	}
	taker, err := getOrderForUpdate(ctx, tx, in.TakerOrderID)
	if err != nil {
		return err
	}

	if err := settleTradeForUser(ctx, tx, maker.UserID, maker.Side, pair, notional, qty); err != nil {
		return err
	}
	if err := settleTradeForUser(ctx, tx, taker.UserID, taker.Side, pair, notional, qty); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func insertTrade(ctx context.Context, tx pgx.Tx, in TradeEventApply) (bool, error) {
	const q = `
INSERT INTO trades (trade_id, symbol, price, quantity, maker_order_id, taker_order_id, wal_seq)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (trade_id) DO NOTHING`

	tag, err := tx.Exec(ctx, q,
		in.TradeID,
		in.Symbol,
		in.Price,
		in.Quantity,
		in.MakerOrderID,
		in.TakerOrderID,
		in.WalSeq,
	)
	if err != nil {
		return false, fmt.Errorf("insert trade: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func settleTradeForUser(ctx context.Context, tx pgx.Tx, userID uint64, side int16, pair symbol.Pair, notional, qty decimal.Decimal) error {
	switch commonv1.Side(side) {
	case commonv1.Side_SIDE_BUY:
		if err := consumeFrozen(ctx, tx, userID, pair.Quote, notional); err != nil {
			return err
		}
		return creditBalance(ctx, tx, userID, pair.Base, qty)
	case commonv1.Side_SIDE_SELL:
		if err := consumeFrozen(ctx, tx, userID, pair.Base, qty); err != nil {
			return err
		}
		return creditBalance(ctx, tx, userID, pair.Quote, notional)
	default:
		return fmt.Errorf("settle trade: invalid side %d", side)
	}
}

func adjustBalance(ctx context.Context, tx pgx.Tx, userID uint64, asset string, delta decimal.Decimal) error {
	if delta.IsZero() {
		return nil
	}

	const selectQ = `
SELECT balance, version FROM account_balances
WHERE user_id = $1 AND asset = $2
FOR UPDATE`

	var balanceStr string
	var version int32
	err := tx.QueryRow(ctx, selectQ, userID, asset).Scan(&balanceStr, &version)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("select balance: %w", err)
	}

	if err == pgx.ErrNoRows {
		if delta.IsNegative() {
			return fmt.Errorf("%w: user=%d asset=%s", ErrInsufficientBalance, userID, asset)
		}
		const ins = `
INSERT INTO account_balances (user_id, asset, balance, frozen)
VALUES ($1, $2, $3, 0)`
		if _, err := tx.Exec(ctx, ins, userID, asset, delta.String()); err != nil {
			return fmt.Errorf("insert balance: %w", err)
		}
		return nil
	}

	balance, err := decimal.NewFromString(balanceStr)
	if err != nil {
		return fmt.Errorf("parse balance: %w", err)
	}
	next := balance.Add(delta)
	if next.IsNegative() {
		return fmt.Errorf("%w: user=%d asset=%s", ErrInsufficientBalance, userID, asset)
	}

	const upd = `
UPDATE account_balances
SET balance = $1, version = version + 1, updated_at = now()
WHERE user_id = $2 AND asset = $3 AND version = $4`
	tag, err := tx.Exec(ctx, upd, next.String(), userID, asset, version)
	if err != nil {
		return fmt.Errorf("update balance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("update balance: concurrent conflict user=%d asset=%s", userID, asset)
	}
	return nil
}

// SeedBalance 为联调/测试注入余额（不存在则创建）。
func (r *Repository) SeedBalance(ctx context.Context, userID uint64, asset, amount string) error {
	const q = `
INSERT INTO account_balances (user_id, asset, balance, frozen)
VALUES ($1, $2, $3, 0)
ON CONFLICT (user_id, asset) DO UPDATE
SET balance = EXCLUDED.balance, updated_at = now()`
	_, err := r.pool.Exec(ctx, q, userID, asset, amount)
	if err != nil {
		return fmt.Errorf("seed balance: %w", err)
	}
	return nil
}
