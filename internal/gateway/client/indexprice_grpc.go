package client

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/config"
	indexv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/index/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type IndexPriceService struct {
	Conn   *grpc.ClientConn
	Client indexv1.IndexPriceServiceClient
}

func ConnectIndexPrice(ctx context.Context, service config.ServiceConfig) (*IndexPriceService, error) {
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
	if err := waitIndexPriceReady(dialCtx, conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc connect %q: %w", service.GRPCAddr, err)
	}
	return &IndexPriceService{
		Conn:   conn,
		Client: indexv1.NewIndexPriceServiceClient(conn),
	}, nil
}

func waitIndexPriceReady(ctx context.Context, conn *grpc.ClientConn) error {
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

func (i *IndexPriceService) Close() error {
	if i == nil || i.Conn == nil {
		return nil
	}
	return i.Conn.Close()
}
