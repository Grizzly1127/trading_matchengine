package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
)

// RedisPublisher 订单状态 WS 推送（§5.6 / rest-api §8.3）。
type RedisPublisher struct {
	rdb *redis.Client
}

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

type orderFrame struct {
	Stream string         `json:"stream"`
	Type   string         `json:"type"`
	Ts     int64          `json:"ts"`
	Data   orderUpdateJSON `json:"data"`
}

type orderUpdateJSON struct {
	OrderID        string `json:"order_id"`
	UserID         uint64 `json:"user_id"`
	Symbol         string `json:"symbol"`
	Status         string `json:"status"`
	Side           int16  `json:"side"`
	OrderType      int16  `json:"order_type"`
	Quantity       string `json:"quantity"`
	FilledQuantity string `json:"filled_quantity"`
	EventType      int16  `json:"event_type"`
	WalSeq         uint64 `json:"wal_seq"`
}

// PublishOrderUpdate 在 match.events 落库后发布到 `order:{user_id}`。
func (p *RedisPublisher) PublishOrderUpdate(ctx context.Context, o *repository.Order, eventType int16, walSeq uint64) error {
	if p == nil || p.rdb == nil {
		return fmt.Errorf("order publisher: not configured")
	}
	if o == nil || o.UserID == 0 {
		return fmt.Errorf("order publisher: order with user_id is required")
	}
	frame := orderFrame{
		Stream: "order",
		Type:   "update",
		Ts:     time.Now().UnixMilli(),
		Data: orderUpdateJSON{
			OrderID:        strconv.FormatUint(o.ID, 10),
			UserID:         o.UserID,
			Symbol:         o.Symbol,
			Status:         o.Status,
			Side:           o.Side,
			OrderType:      o.OrderType,
			Quantity:       o.Quantity,
			FilledQuantity: o.FilledQuantity,
			EventType:      eventType,
			WalSeq:         walSeq,
		},
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("order publisher: marshal: %w", err)
	}
	ch := fmt.Sprintf("order:%d", o.UserID)
	return p.rdb.Publish(ctx, ch, string(payload))
}
