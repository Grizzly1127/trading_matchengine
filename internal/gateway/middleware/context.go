package middleware

import (
	"context"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/requestctx"
)

// WithRequestID 将请求 ID 写入 context。
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return requestctx.WithRequestID(ctx, requestID)
}

// RequestIDFromContext 读取请求 ID。
func RequestIDFromContext(ctx context.Context) string {
	return requestctx.RequestID(ctx)
}

// WithUserID 将认证用户 ID 写入 context。
func WithUserID(ctx context.Context, userID uint64) context.Context {
	return requestctx.WithUserID(ctx, userID)
}

// UserIDFromContext 读取用户 ID。
func UserIDFromContext(ctx context.Context) uint64 {
	return requestctx.UserID(ctx)
}
