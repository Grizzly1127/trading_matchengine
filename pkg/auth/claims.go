package auth

import "context"

type contextKey int

const claimsKey contextKey = 1

// Claims 内网调用方身份（非终端用户）。
type Claims struct {
	Subject string
	Scopes  []string
	Method  string // static | jwt
}

// WithClaims 写入 context。
func WithClaims(ctx context.Context, c Claims) context.Context {
	return context.WithValue(ctx, claimsKey, c)
}

// ClaimsFromContext 读取鉴权结果；未鉴权返回 false。
func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(claimsKey).(Claims)
	return c, ok
}

// HasScopes 是否包含全部 required scope。
func HasScopes(c Claims, required ...string) bool {
	if len(required) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(c.Scopes))
	for _, s := range c.Scopes {
		set[s] = struct{}{}
	}
	for _, need := range required {
		if _, ok := set[need]; !ok {
			return false
		}
	}
	return true
}
