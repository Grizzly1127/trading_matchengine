package repository

import "github.com/Grizzly1127/trading_matchengine/internal/order/outbox"

var _ outbox.Store = (*Repository)(nil)
