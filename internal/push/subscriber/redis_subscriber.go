package subscriber

import (
	"context"
	"fmt"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/limits"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/Grizzly1127/trading_matchengine/pkg/tickerall"
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
	sub, err := r.Redis.PSubscribe(ctx, "depth:*", "ticker:*", "trade:*", "kline:*", "index:*", "ticker@all:*", "order:*")
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
		payload := []byte(msg.Payload)
		if limits.IsTickerAllChannel(msg.Channel) && !tickerall.IsWSFrame(payload) {
			r.Log.Warn().Str("channel", msg.Channel).Msg("drop non §8.2 ticker@all frame")
			continue
		}
		if uid, ok := hub.ParseOrderChannel(msg.Channel); ok {
			r.Hub.BroadcastOrder(uid, payload)
			continue
		}
		r.Hub.Broadcast(msg.Channel, payload)
	}
}
