package client

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultDialTimeout = 10 * time.Second

// OrderGRPC 封装 Order Service gRPC 连接与客户端。
type OrderGRPC struct {
	Conn          *grpc.ClientConn
	OrderClient   orderv1.OrderServiceClient
	BalanceClient orderv1.BalanceServiceClient
}

// Connect 建立 gRPC 连接并等待 Ready（带超时）。
func ConnectOrder(ctx context.Context, service config.ServiceConfig) (*OrderGRPC, error) {
	dialTimeout := time.Duration(service.GRPCDialSec) * time.Second
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}

	conn, err := grpc.NewClient(service.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc new client %q: %w", service.GRPCAddr, err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	if err := waitReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc connect %q: %w", service.GRPCAddr, err)
	}

	return &OrderGRPC{
		Conn:          conn,
		OrderClient:   orderv1.NewOrderServiceClient(conn),
		BalanceClient: orderv1.NewBalanceServiceClient(conn),
	}, nil
}

func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
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

// Close 关闭底层连接。
func (o *OrderGRPC) Close() error {
	if o == nil || o.Conn == nil {
		return nil
	}
	return o.Conn.Close()
}
