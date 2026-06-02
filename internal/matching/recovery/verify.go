package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/presence"
	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
)

// ReconcileOrder DB 侧待对账订单。
type ReconcileOrder struct {
	OrderID uint64
	Status  string
}

// OrderQuerier 向 Order Service 拉取对账数据（§5.6）。
type OrderQuerier interface {
	ListReconcileOrders(ctx context.Context, symbol string) ([]ReconcileOrder, error)
	GetOrderStatuses(ctx context.Context, symbol string, orderIDs []uint64) (map[uint64]string, error)
}

// SymbolDiff 单交易对 diff 结果。
type SymbolDiff struct {
	Symbol     string
	OnlyInDB   []uint64
	OnlyInBook []uint64
}

// HasMismatch 是否存在不一致。
func (d SymbolDiff) HasMismatch() bool {
	return len(d.OnlyInDB) > 0 || len(d.OnlyInBook) > 0
}

// VerifyConfig 启动对账参数。
type VerifyConfig struct {
	Timeout time.Duration
}

// VerifyAll 按 symbol 与 Order Service 对账；返回存在 diff 的 symbol 列表。
func VerifyAll(ctx context.Context, eng *Engine, q OrderQuerier, symbols []string, cfg VerifyConfig) ([]SymbolDiff, error) {
	if eng == nil {
		return nil, fmt.Errorf("recovery verify: engine is nil")
	}
	if q == nil {
		return nil, fmt.Errorf("recovery verify: order querier is nil")
	}
	if len(symbols) == 0 {
		return nil, nil
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var mismatches []SymbolDiff
	for _, sym := range symbols {
		select {
		case <-ctx.Done():
			return mismatches, ctx.Err()
		default:
		}
		diff, err := verifySymbol(ctx, eng, q, sym)
		if err != nil {
			return mismatches, fmt.Errorf("recovery verify %s: %w", sym, err)
		}
		if diff.HasMismatch() {
			mismatches = append(mismatches, diff)
		}
	}
	return mismatches, nil
}

func verifySymbol(ctx context.Context, eng *Engine, q OrderQuerier, symbol string) (SymbolDiff, error) {
	diff := SymbolDiff{Symbol: symbol}

	dbRows, err := q.ListReconcileOrders(ctx, symbol)
	if err != nil {
		return diff, err
	}
	bookIDs := eng.ActiveOrderIDs(symbol)
	bookSet := make(map[uint64]struct{}, len(bookIDs))
	for _, id := range bookIDs {
		bookSet[id] = struct{}{}
	}

	dbMap := make(map[uint64]string, len(dbRows))
	for _, row := range dbRows {
		dbMap[row.OrderID] = row.Status
	}

	for id, st := range dbMap {
		if _, inBook := bookSet[id]; inBook {
			continue
		}
		if isDBOnlyMismatch(eng, symbol, id, st) {
			diff.OnlyInDB = append(diff.OnlyInDB, id)
		}
	}

	var bookOnly []uint64
	for id := range bookSet {
		if _, inDB := dbMap[id]; inDB {
			continue
		}
		bookOnly = append(bookOnly, id)
	}
	if len(bookOnly) > 0 {
		statuses, err := q.GetOrderStatuses(ctx, symbol, bookOnly)
		if err != nil {
			return diff, err
		}
		for _, id := range bookOnly {
			st, ok := statuses[id]
			if !ok || status.IsTerminal(st) {
				diff.OnlyInBook = append(diff.OnlyInBook, id)
				continue
			}
			// ACCEPTED/CANCELING 等未纳入 PENDING/PARTIAL 查询集，盘口存在视为一致。
			if st == status.Pending || st == status.Partial {
				diff.OnlyInBook = append(diff.OnlyInBook, id)
			}
		}
	}
	return diff, nil
}

func isDBOnlyMismatch(eng *Engine, symbol string, orderID uint64, dbStatus string) bool {
	switch dbStatus {
	case status.Partial:
		return true
	case status.Pending:
		if eng.LookupOrderPresence(symbol, orderID) == presence.InOrderbook {
			return true
		}
		return false
	default:
		return false
	}
}

// ApplyReadOnly 将 diff 涉及 symbol 置为只读拒单。
func ApplyReadOnly(eng *Engine, diffs []SymbolDiff, reason string) {
	if eng == nil {
		return
	}
	for _, d := range diffs {
		if !d.HasMismatch() {
			continue
		}
		eng.SetSymbolReadOnly(d.Symbol, reason)
	}
}
