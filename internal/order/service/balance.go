package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// BalanceStore 资产读写接口。
type BalanceStore interface {
	GetBalance(ctx context.Context, userID uint64, asset string) (*repository.AccountBalance, error)
	ListBalances(ctx context.Context, userID uint64) ([]repository.AccountBalance, error)
	UpdateBalance(ctx context.Context, in repository.UpdateBalanceInput) (*repository.AccountBalance, error)
}

// BalanceService 资产业务逻辑。
type BalanceService struct {
	Repo BalanceStore
}

// GetBalance 查询单资产余额。
func (s *BalanceService) GetBalance(ctx context.Context, req *orderv1.GetBalanceRequest) (*orderv1.GetBalanceResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("balance service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}
	userID, asset, err := validateBalanceUserAsset(req.GetUserId(), req.GetAsset())
	if err != nil {
		return nil, err
	}

	row, err := s.Repo.GetBalance(ctx, userID, asset)
	if err != nil {
		if errors.Is(err, repository.ErrBalanceNotFound) {
			return nil, fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		return nil, err
	}
	return &orderv1.GetBalanceResponse{Balance: toBalancePB(row)}, nil
}

// ListBalances 查询用户全部资产余额。
func (s *BalanceService) ListBalances(ctx context.Context, req *orderv1.ListBalancesRequest) (*orderv1.ListBalancesResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("balance service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}
	if req.GetUserId() == 0 {
		return nil, fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}

	rows, err := s.Repo.ListBalances(ctx, req.GetUserId())
	if err != nil {
		return nil, err
	}
	out := make([]*orderv1.Balance, 0, len(rows))
	for i := range rows {
		out = append(out, toBalancePB(&rows[i]))
	}
	return &orderv1.ListBalancesResponse{Balances: out}, nil
}

// UpdateBalance 调整可用余额（充值/扣款等），幂等键 business + business_id。
func (s *BalanceService) UpdateBalance(ctx context.Context, req *orderv1.UpdateBalanceRequest) (*orderv1.UpdateBalanceResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, fmt.Errorf("balance service not configured")
	}
	if req == nil {
		return nil, fmt.Errorf("%w: request is nil", ErrInvalidArgument)
	}
	userID, asset, err := validateBalanceUserAsset(req.GetUserId(), req.GetAsset())
	if err != nil {
		return nil, err
	}

	business := strings.TrimSpace(req.GetBusiness())
	if business == "" {
		return nil, fmt.Errorf("%w: business is required", ErrInvalidArgument)
	}
	if len(business) > 32 {
		return nil, fmt.Errorf("%w: business too long", ErrInvalidArgument)
	}

	change, err := parseDecimalField(req.GetChange(), "change")
	if err != nil {
		return nil, err
	}
	if change.IsZero() {
		return nil, fmt.Errorf("%w: change must not be zero", ErrInvalidArgument)
	}

	row, err := s.Repo.UpdateBalance(ctx, repository.UpdateBalanceInput{
		UserID:     userID,
		Asset:      asset,
		Business:   business,
		BusinessID: req.GetBusinessId(),
		Change:     change,
	})
	if err != nil {
		if errors.Is(err, repository.ErrInsufficientBalance) {
			return nil, fmt.Errorf("%w: %v", ErrFailedPrecondition, err)
		}
		return nil, err
	}
	return &orderv1.UpdateBalanceResponse{Balance: toBalancePB(row)}, nil
}

func validateBalanceUserAsset(userID uint64, asset string) (uint64, string, error) {
	if userID == 0 {
		return 0, "", fmt.Errorf("%w: user_id is required", ErrInvalidArgument)
	}
	asset = strings.TrimSpace(strings.ToUpper(asset))
	if asset == "" {
		return 0, "", fmt.Errorf("%w: asset is required", ErrInvalidArgument)
	}
	if len(asset) > 16 {
		return 0, "", fmt.Errorf("%w: asset too long", ErrInvalidArgument)
	}
	return userID, asset, nil
}

func parseDecimalField(d *commonv1.Decimal, field string) (decimal.Decimal, error) {
	if d == nil || strings.TrimSpace(d.GetValue()) == "" {
		return decimal.Zero, fmt.Errorf("%w: %s is required", ErrInvalidArgument, field)
	}
	v, err := decimal.NewFromString(strings.TrimSpace(d.GetValue()))
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: %s must be a decimal", ErrInvalidArgument, field)
	}
	return v, nil
}

func toBalancePB(row *repository.AccountBalance) *orderv1.Balance {
	if row == nil {
		return nil
	}
	return &orderv1.Balance{
		Asset: row.Asset,
		Balance: &commonv1.Decimal{
			Value: row.Balance.String(),
		},
		Frozen: &commonv1.Decimal{
			Value: row.Frozen.String(),
		},
		Available: &commonv1.Decimal{
			Value: row.Available().String(),
		},
	}
}
