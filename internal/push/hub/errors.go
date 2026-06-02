package hub

import "errors"

var (
	// ErrTooManyConnections 同一 subject 超过并发连接上限。
	ErrTooManyConnections = errors.New("too many websocket connections for this account")
)
