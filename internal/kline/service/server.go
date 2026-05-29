package service

import (
	"context"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/kline/aggregator"
	"github.com/Grizzly1127/trading_matchengine/internal/kline/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/bar"
	"github.com/Grizzly1127/trading_matchengine/pkg/kline/interval"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server 实现 KlineService gRPC。
type Server struct {
	klinev1.UnimplementedKlineServiceServer
	Repo *repository.Repository
	Agg  *aggregator.Aggregator
}

// NewServer 创建 gRPC 服务。
func NewServer(repo *repository.Repository, agg *aggregator.Aggregator) *Server {
	return &Server{Repo: repo, Agg: agg}
}

// GetKlines 查询历史闭合 K 线，并附带当前未闭合 bar（若存在）。
func (s *Server) GetKlines(ctx context.Context, req *klinev1.GetKlinesRequest) (*klinev1.GetKlinesResponse, error) {
	if s == nil || s.Repo == nil {
		return nil, status.Error(codes.Unavailable, "kline repository not configured")
	}
	symbol := req.GetSymbol()
	if symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	iv, err := interval.Parse(req.GetInterval())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	limit := int(req.GetLimit())
	q := repository.ListQuery{
		Symbol:   symbol,
		Interval: iv,
		Limit:    limit,
	}
	if t := req.GetStartTime(); t != nil {
		st := t.AsTime().UTC()
		q.StartTime = &st
	}
	if t := req.GetEndTime(); t != nil {
		et := t.AsTime().UTC()
		q.EndTime = &et
	}

	closed, err := s.Repo.ListClosed(ctx, q)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list klines: %v", err)
	}

	out := make([]*klinev1.Kline, 0, len(closed)+1)
	for i := len(closed) - 1; i >= 0; i-- {
		out = append(out, closedBarToPB(closed[i]))
	}

	if s.Agg != nil {
		if b, ok := s.Agg.SnapshotOpen(symbol, iv); ok {
			openPB := openBarToPB(iv, b)
			if len(out) == 0 || !sameOpenTime(out[len(out)-1], openPB) {
				out = append(out, openPB)
			}
		}
	}

	return &klinev1.GetKlinesResponse{Klines: out}, nil
}

func closedBarToPB(rec repository.ClosedBar) *klinev1.Kline {
	return &klinev1.Kline{
		OpenTime:  timestamppb.New(rec.OpenTime),
		CloseTime: timestamppb.New(rec.CloseTime),
		Open:      &commonv1.Decimal{Value: rec.Open},
		High:      &commonv1.Decimal{Value: rec.High},
		Low:       &commonv1.Decimal{Value: rec.Low},
		Close:     &commonv1.Decimal{Value: rec.Close},
		Volume:    &commonv1.Decimal{Value: rec.Volume},
		IsClosed:  true,
	}
}

func openBarToPB(iv interval.Interval, b bar.OHLCV) *klinev1.Kline {
	openT := time.UnixMilli(b.OpenTimeMs).UTC()
	closeT := time.UnixMilli(iv.CloseTimeMs(b.OpenTimeMs)).UTC()
	return &klinev1.Kline{
		OpenTime:  timestamppb.New(openT),
		CloseTime: timestamppb.New(closeT),
		Open:      &commonv1.Decimal{Value: b.Open.String()},
		High:      &commonv1.Decimal{Value: b.High.String()},
		Low:       &commonv1.Decimal{Value: b.Low.String()},
		Close:     &commonv1.Decimal{Value: b.Close.String()},
		Volume:    &commonv1.Decimal{Value: b.Volume.String()},
		IsClosed:  false,
	}
}

func sameOpenTime(a, b *klinev1.Kline) bool {
	if a == nil || b == nil {
		return false
	}
	return a.GetOpenTime().AsTime().Equal(b.GetOpenTime().AsTime())
}
