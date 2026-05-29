package client

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// KlineGRPC 封装 Kline Service gRPC 连接。
type KlineGRPC struct {
	Conn   *grpc.ClientConn
	Client klinev1.KlineServiceClient
}

// ConnectKline 连接 Kline gRPC 服务。
func ConnectKline(ctx context.Context, service config.ServiceConfig) (*KlineGRPC, error) {
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
	if err := waitKlineReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc connect %q: %w", service.GRPCAddr, err)
	}
	return &KlineGRPC{
		Conn:   conn,
		Client: klinev1.NewKlineServiceClient(conn),
	}, nil
}

func waitKlineReady(ctx context.Context, conn *grpc.ClientConn) error {
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

func (k *KlineGRPC) Close() error {
	if k == nil || k.Conn == nil {
		return nil
	}
	return k.Conn.Close()
}
