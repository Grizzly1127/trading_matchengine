package orderclient

import (
	"context"
	"fmt"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/matching/recovery"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client Order Admin gRPC 客户端（启动对账 §5.6）。
type Client struct {
	Admin orderv1.OrderAdminServiceClient
	conn  *grpc.ClientConn
}

// Connect 建立连接。
func Connect(ctx context.Context, addr string, dialTimeout time.Duration) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("order client: addr is required")
	}
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("order client dial %s: %w", addr, err)
	}
	return &Client{
		Admin: orderv1.NewOrderAdminServiceClient(conn),
		conn:  conn,
	}, nil
}

// Close 关闭连接。
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// ListReconcileOrders 实现 recovery.OrderQuerier。
func (c *Client) ListReconcileOrders(ctx context.Context, symbol string) ([]recovery.ReconcileOrder, error) {
	if c == nil || c.Admin == nil {
		return nil, fmt.Errorf("order client: not connected")
	}
	resp, err := c.Admin.ListReconcileOrders(ctx, &orderv1.ListReconcileOrdersRequest{Symbol: symbol})
	if err != nil {
		return nil, err
	}
	out := make([]recovery.ReconcileOrder, 0, len(resp.GetOrders()))
	for _, o := range resp.GetOrders() {
		out = append(out, recovery.ReconcileOrder{
			OrderID: o.GetOrderId(),
			Status:  o.GetStatus(),
		})
	}
	return out, nil
}

// GetOrderStatuses 实现 recovery.OrderQuerier。
func (c *Client) GetOrderStatuses(ctx context.Context, symbol string, orderIDs []uint64) (map[uint64]string, error) {
	if c == nil || c.Admin == nil {
		return nil, fmt.Errorf("order client: not connected")
	}
	resp, err := c.Admin.GetOrderStatuses(ctx, &orderv1.GetOrderStatusesRequest{
		Symbol:   symbol,
		OrderIds: orderIDs,
	})
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string, len(resp.GetOrders()))
	for _, o := range resp.GetOrders() {
		if o.GetFound() {
			out[o.GetOrderId()] = o.GetStatus()
		}
	}
	return out, nil
}
