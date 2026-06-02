package matching

import (
	"context"
	"fmt"
	"time"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client 撮合对账 gRPC 客户端（MatchingAdminService）。
type Client struct {
	conn    *grpc.ClientConn
	Admin   matchingv1.MatchingAdminServiceClient
	Timeout time.Duration
}

// Connect 建立内网连接。
func Connect(ctx context.Context, addr string, dialTimeout time.Duration) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("matching: grpc addr is required")
	}
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("matching: dial %s: %w", addr, err)
	}
	return &Client{
		conn:    conn,
		Admin:   matchingv1.NewMatchingAdminServiceClient(conn),
		Timeout: 5 * time.Second,
	}, nil
}

// Close 关闭连接。
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// GetOrderPresence 查询单笔订单在撮合内存中的存在性。
func (c *Client) GetOrderPresence(ctx context.Context, symbol string, orderID uint64) (matchingv1.OrderPresence, error) {
	if c == nil || c.Admin == nil {
		return matchingv1.OrderPresence_ORDER_PRESENCE_UNSPECIFIED, fmt.Errorf("matching client not configured")
	}
	reqCtx := ctx
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	resp, err := c.Admin.GetOrderPresence(reqCtx, &matchingv1.GetOrderPresenceRequest{
		Symbol:  symbol,
		OrderId: orderID,
	})
	if err != nil {
		return matchingv1.OrderPresence_ORDER_PRESENCE_UNSPECIFIED, err
	}
	return resp.GetPresence(), nil
}
