package outbox

import (
	"google.golang.org/protobuf/proto"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

// BuildCancelOrderPayload 构建 CancelOrderCommand 载荷；command_id 使用 outbox.id。
func BuildCancelOrderPayload(symbol string, orderID, commandID uint64) ([]byte, error) {
	env := &matchingv1.OrderCommandEnvelope{
		Command: &matchingv1.OrderCommandEnvelope_CancelOrder{
			CancelOrder: &matchingv1.CancelOrderCommand{
				CommandId: commandID,
				Symbol:    symbol,
				OrderId:   orderID,
			},
		},
	}
	return proto.Marshal(env)
}
