package client

import (
	"context"
	"fmt"
	"time"

	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultDialTimeout = 10 * time.Second

// Order 封装 Order Service gRPC 连接与客户端。
type Order struct {
	Conn   *grpc.ClientConn
	Client orderv1.OrderServiceClient
}

// Connect 建立 gRPC 连接并等待 Ready（带超时）。
func Connect(ctx context.Context, addr string, dialTimeout time.Duration) (*Order, error) {
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

	if err := waitReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc connect %q: %w", addr, err)
	}

	return &Order{
		Conn:   conn,
		Client: orderv1.NewOrderServiceClient(conn),
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
func (o *Order) Close() error {
	if o == nil || o.Conn == nil {
		return nil
	}
	return o.Conn.Close()
}
