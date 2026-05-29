package subscriber

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/rs/zerolog"
)

type RedisFanout struct {
	Redis *redis.Client
	Hub   *hub.Hub
	Log   zerolog.Logger
}

func (r *RedisFanout) Run(ctx context.Context) error {
	if r == nil || r.Redis == nil || r.Hub == nil {
		return fmt.Errorf("push subscriber not configured")
	}
	sub, err := r.Redis.PSubscribe(ctx, "depth:*", "ticker:*", "trade:*", "kline:*", "index:*", "ticker@all:*")
	if err != nil {
		return err
	}
	defer sub.Close()

	for {
		msg, err := sub.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		r.Hub.Broadcast(msg.Channel, []byte(msg.Payload))
	}
}
