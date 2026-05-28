package client

import (
	"context"
	"fmt"
	"time"

	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// MarketDataGRPC 封装 MarketData Service gRPC 连接与客户端。
type MarketDataGRPC struct {
	Conn   *grpc.ClientConn
	Client marketdatav1.MarketDataServiceClient
}

func ConnectMarketData(ctx context.Context, addr string, dialTimeout time.Duration) (*MarketDataGRPC, error) {
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc new client %q: %w", addr, err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	if err := waitMarketDataReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc connect %q: %w", addr, err)
	}

	return &MarketDataGRPC{
		Conn:   conn,
		Client: marketdatav1.NewMarketDataServiceClient(conn),
	}, nil
}

func waitMarketDataReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("connection state %s", conn.GetState())
		}
	}
}

func (m *MarketDataGRPC) Close() error {
	if m == nil || m.Conn == nil {
		return nil
	}
	return m.Conn.Close()
}
