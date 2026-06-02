package handler

import (
	"context"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/marketdata/store"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	marketdatav1.UnimplementedMarketDataServiceServer
	store *store.Store
}

func NewServer(st *store.Store) *Server {
	return &Server{store: st}
}

func (s *Server) GetOrderBook(ctx context.Context, req *marketdatav1.GetOrderBookRequest) (*marketdatav1.GetOrderBookResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "marketdata store not configured")
	}
	symbol := req.GetSymbol()
	if symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	snap, ok := s.store.SnapshotOrderBook(symbol, int(req.GetLimit()))
	if !ok {
		return nil, status.Errorf(codes.NotFound, "orderbook %s not found", symbol)
	}
	return &marketdatav1.GetOrderBookResponse{
		Symbol:       snap.Symbol,
		LastUpdateId: snap.LastUpdateID,
		Bids:         toPBLevels(snap.Bids),
		Asks:         toPBLevels(snap.Asks),
		Timestamp:    millisToTimestamp(snap.UpdatedAtMs),
	}, nil
}

func (s *Server) GetTicker(ctx context.Context, req *marketdatav1.GetTickerRequest) (*marketdatav1.GetTickerResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "marketdata store not configured")
	}
	if req.GetSymbol() != "" {
		t, ok := s.store.SnapshotTicker(req.GetSymbol())
		if !ok || t.UpdatedAtMs == 0 {
			return nil, status.Errorf(codes.NotFound, "ticker %s not found", req.GetSymbol())
		}
		return &marketdatav1.GetTickerResponse{Ticker: toPBTicker(req.GetSymbol(), t)}, nil
	}

	symbols := req.GetSymbols()
	if len(symbols) == 0 {
		return nil, status.Error(codes.InvalidArgument, "symbol or symbols is required")
	}
	if len(symbols) > 100 {
		return nil, status.Error(codes.InvalidArgument, "symbols cannot exceed 100")
	}
	items := make([]*marketdatav1.Ticker, 0, len(symbols))
	for _, symbol := range symbols {
		t, ok := s.store.SnapshotTicker(symbol)
		if !ok || t.UpdatedAtMs == 0 {
			continue
		}
		items = append(items, toPBTicker(symbol, t))
	}
	return &marketdatav1.GetTickerResponse{Items: items}, nil
}

func (s *Server) GetReferencePrice(ctx context.Context, req *marketdatav1.GetReferencePriceRequest) (*marketdatav1.GetReferencePriceResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "marketdata store not configured")
	}
	if req.GetSymbol() == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	kind, err := toStoreReferenceKind(req.GetKind())
	if err != nil {
		return nil, err
	}
	ref, ok := s.store.ReferencePrice(req.GetSymbol(), kind)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "reference price %s not available", req.GetSymbol())
	}
	return &marketdatav1.GetReferencePriceResponse{
		Price:        dec(ref.Price),
		UpdatedAtMs:  ref.UpdatedAtMs,
		ResolvedKind: toPBReferenceKind(ref.Kind),
	}, nil
}

func (s *Server) GetTickerAllSnapshot(ctx context.Context, req *marketdatav1.GetTickerAllSnapshotRequest) (*marketdatav1.GetTickerAllSnapshotResponse, error) {
	_ = ctx
	if s == nil || s.store == nil {
		return nil, status.Error(codes.Unavailable, "marketdata store not configured")
	}
	snap := s.store.BuildTickerAllSnapshot(req.GetQuoteAsset())
	if req.GetSnapshotId() != "" && req.GetSnapshotId() == snap.SnapshotID {
		return &marketdatav1.GetTickerAllSnapshotResponse{
			SnapshotId:  snap.SnapshotID,
			Count:       int32(snap.Count),
			NotModified: true,
		}, nil
	}
	pbItems := make([]*marketdatav1.TickerAllItem, 0, len(snap.Items))
	for _, item := range snap.Items {
		pbItems = append(pbItems, &marketdatav1.TickerAllItem{
			Symbol:             item.Symbol,
			LastPrice:          dec(store.FormatDecimal(item.LastPrice)),
			Volume:             dec(store.FormatDecimal(item.Volume)),
			QuoteVolume:        dec(store.FormatDecimal(item.QuoteVolume)),
			PriceChangePercent: dec(store.FormatPercent(item.PriceChangePercent)),
			Timestamp:          millisToTimestamp(item.UpdatedAtMs),
		})
	}
	return &marketdatav1.GetTickerAllSnapshotResponse{
		SnapshotId:   snap.SnapshotID,
		SnapshotTime: millisToTimestamp(snap.SnapshotTime),
		Count:        int32(snap.Count),
		Items:        pbItems,
		NotModified:  false,
	}, nil
}

func toPBLevels(levels []store.PriceLevel) []*marketdatav1.PriceLevel {
	out := make([]*marketdatav1.PriceLevel, 0, len(levels))
	for _, lv := range levels {
		out = append(out, &marketdatav1.PriceLevel{
			Price:    dec(lv.Price),
			Quantity: dec(lv.Quantity),
		})
	}
	return out
}

func toPBTicker(symbol string, t store.TickerState) *marketdatav1.Ticker {
	return &marketdatav1.Ticker{
		Symbol:             symbol,
		LastPrice:          dec(store.FormatDecimal(t.LastPrice)),
		OpenPrice:          dec(store.FormatDecimal(t.OpenPrice)),
		HighPrice:          dec(store.FormatDecimal(t.HighPrice)),
		LowPrice:           dec(store.FormatDecimal(t.LowPrice)),
		Volume:             dec(store.FormatDecimal(t.Volume)),
		QuoteVolume:        dec(store.FormatDecimal(t.QuoteVolume)),
		PriceChangePercent: dec(store.FormatPercent(t.PriceChangePercent)),
		Timestamp:          millisToTimestamp(t.UpdatedAtMs),
	}
}

func toStoreReferenceKind(kind marketdatav1.ReferencePriceKind) (store.ReferencePriceKind, error) {
	switch kind {
	case marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_UNSPECIFIED,
		marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_BEST_ASK:
		return store.ReferencePriceBestAsk, nil
	case marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_MARK:
		return store.ReferencePriceMark, nil
	case marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_LAST:
		return store.ReferencePriceLast, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "unknown reference price kind")
	}
}

func toPBReferenceKind(kind store.ReferencePriceKind) marketdatav1.ReferencePriceKind {
	switch kind {
	case store.ReferencePriceBestAsk:
		return marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_BEST_ASK
	case store.ReferencePriceMark:
		return marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_MARK
	case store.ReferencePriceLast:
		return marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_LAST
	default:
		return marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_UNSPECIFIED
	}
}

func millisToTimestamp(ms int64) *timestamppb.Timestamp {
	if ms <= 0 {
		return timestamppb.Now()
	}
	return timestamppb.New(time.UnixMilli(ms))
}

func dec(value string) *commonv1.Decimal {
	return &commonv1.Decimal{Value: value}
}
