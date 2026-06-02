package service

import (
	"context"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/indexprice/store"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server 实现 IndexPriceService gRPC。
type Server struct {
	indexv1.UnimplementedIndexPriceServiceServer
	Store *store.Store
}

func NewServer(st *store.Store) *Server {
	return &Server{Store: st}
}

func (s *Server) GetIndexPrice(ctx context.Context, req *indexv1.GetIndexPriceRequest) (*indexv1.GetIndexPriceResponse, error) {
	_ = ctx
	if s == nil || s.Store == nil {
		return nil, status.Error(codes.Unavailable, "index store not configured")
	}
	symbol := req.GetSymbol()
	if symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	snap, ok := s.Store.Get(symbol)
	if !ok {
		return nil, status.Error(codes.NotFound, "index price not available")
	}
	return &indexv1.GetIndexPriceResponse{
		IndexPrice: &indexv1.IndexPrice{
			Symbol:    snap.Symbol,
			Price:     &commonv1.Decimal{Value: snap.Price.String()},
			UpdatedAt: timestamppb.New(snap.Updated),
			Sources:   snap.Sources,
			Stale:     snap.Stale || isStale(snap.Updated, time.Now().UTC()),
		},
	}, nil
}

func isStale(updated, now time.Time) bool {
	return now.Sub(updated) > 60*time.Second
}
