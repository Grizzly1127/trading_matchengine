package outbox

import (
	"testing"

	"google.golang.org/protobuf/proto"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

func TestBuildCancelOrderPayload(t *testing.T) {
	payload, err := BuildCancelOrderPayload("BTC-USDT", 42, 99)
	if err != nil {
		t.Fatal(err)
	}
	var env matchingv1.OrderCommandEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		t.Fatal(err)
	}
	cmd := env.GetCancelOrder()
	if cmd == nil || cmd.GetOrderId() != 42 || cmd.GetCommandId() != 99 {
		t.Fatalf("cmd=%+v", cmd)
	}
}
