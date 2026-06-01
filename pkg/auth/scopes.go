package auth

// 服务间 JWT scope（Gateway / Push 共用）。
const (
	ScopeOrdersRead    = "orders:read"
	ScopeOrdersWrite   = "orders:write"
	ScopeBalancesRead  = "balances:read"
	ScopeBalancesAdmin = "balances:admin"
	ScopeMarketRead    = "market:read"
	ScopePushConnect   = "push:connect"
)

// AllScopes 开发 static 模式下放行的全部 scope。
var AllScopes = []string{
	ScopeOrdersRead,
	ScopeOrdersWrite,
	ScopeBalancesRead,
	ScopeBalancesAdmin,
	ScopeMarketRead,
	ScopePushConnect,
}
