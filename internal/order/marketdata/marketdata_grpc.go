package marketdata

import (
	"context"
	"fmt"
	"time"

	marketdatav1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/marketdata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultDialTimeout = 10 * time.Second

type MarketDataGRPC struct {
	Conn    *grpc.ClientConn
	Client  marketdatav1.MarketDataServiceClient
	Timeout time.Duration
}

func Connect(ctx context.Context, addr string, dialTimeout time.Duration) (*MarketDataGRPC, error) {
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

	return &MarketDataGRPC{
		Conn:    conn,
		Client:  marketdatav1.NewMarketDataServiceClient(conn),
		Timeout: time.Second,
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
func (o *MarketDataGRPC) Close() error {
	if o == nil || o.Conn == nil {
		return nil
	}
	return o.Conn.Close()
}

func (o *MarketDataGRPC) GetReferencePrice(ctx context.Context, symbol string) (string, error) {
	if o == nil || o.Client == nil {
		return "", fmt.Errorf("marketdata client not configured")
	}
	if symbol == "" {
		return "", fmt.Errorf("symbol is required")
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := o.Client.GetReferencePrice(reqCtx, &marketdatav1.GetReferencePriceRequest{
		Symbol: symbol,
		Kind:   marketdatav1.ReferencePriceKind_REFERENCE_PRICE_KIND_BEST_ASK,
	})
	if err != nil {
		return "", err
	}
	if resp.GetPrice() == nil || resp.GetPrice().GetValue() == "" {
		return "", fmt.Errorf("reference price is empty")
	}
	return resp.GetPrice().GetValue(), nil
}
