package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Grizzly1127/trading_matchengine/internal/order/outbox"
)

// RelayStore 使用 Relay 专用连接池实现 outbox.Store，与 API 写路径隔离。
type RelayStore struct {
	repo *Repository
}

// SetRelayPool 配置 Relay 专用连接池；未设置时 Relay 与 API 共用 pool。
func (r *Repository) SetRelayPool(pool *pgxpool.Pool) {
	if r != nil {
		r.relayPool = pool
	}
}

func (r *Repository) relayDB() *pgxpool.Pool {
	if r != nil && r.relayPool != nil {
		return r.relayPool
	}
	return r.pool
}

// RelayStore 返回绑定 Relay 连接池的 Store 实现。
func (r *Repository) RelayStore() outbox.Store {
	return &RelayStore{repo: r}
}

func (s *RelayStore) ClaimUnpublished(ctx context.Context, limit int) (outbox.ClaimHandle, error) {
	return s.repo.claimUnpublished(ctx, s.repo.relayDB(), limit)
}

func (s *RelayStore) MarkPublished(ctx context.Context, id uint64) error {
	return s.repo.markPublished(ctx, s.repo.relayDB(), id)
}

func (s *RelayStore) MarkPublishedBatch(ctx context.Context, ids []uint64) error {
	return s.repo.markPublishedBatch(ctx, s.repo.relayDB(), ids)
}

func (s *RelayStore) IncrementRetry(ctx context.Context, id uint64) error {
	return s.repo.incrementRetry(ctx, s.repo.relayDB(), id)
}

func (s *RelayStore) GetOrderStatus(ctx context.Context, orderID uint64) (string, error) {
	statuses, err := s.GetOrderStatusesBatch(ctx, []uint64{orderID})
	if err != nil {
		return "", err
	}
	status, ok := statuses[orderID]
	if !ok {
		return "", fmt.Errorf("order %d not found", orderID)
	}
	return status, nil
}

func (s *RelayStore) GetOrderStatusesBatch(ctx context.Context, orderIDs []uint64) (map[uint64]string, error) {
	return s.repo.getOrderStatusesBatch(ctx, s.repo.relayDB(), orderIDs)
}
