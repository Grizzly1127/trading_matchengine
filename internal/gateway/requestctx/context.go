package requestctx

import "context"

type ctxKey int

const (
	keyRequestID ctxKey = iota + 1
	keyUserID
)

// WithRequestID 将请求 ID 写入 context。
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, keyRequestID, requestID)
}

// RequestID 读取请求 ID。
func RequestID(ctx context.Context) string {
	if v, ok := ctx.Value(keyRequestID).(string); ok {
		return v
	}
	return ""
}

// WithUserID 将认证用户 ID 写入 context。
func WithUserID(ctx context.Context, userID uint64) context.Context {
	return context.WithValue(ctx, keyUserID, userID)
}

// UserID 读取用户 ID；未认证时为 0。
func UserID(ctx context.Context) uint64 {
	if v, ok := ctx.Value(keyUserID).(uint64); ok {
		return v
	}
	return 0
}
