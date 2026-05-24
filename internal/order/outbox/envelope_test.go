package outbox

import (
	"testing"

	"google.golang.org/protobuf/proto"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

func TestBuildNewOrderPayload(t *testing.T) {
	price := "65000.5"
	order := OrderSnapshot{
		ID:            42,
		ClientOrderID: "demo-001",
		Symbol:        "BTC-USDT",
		Side:          1,
		OrderType:     1,
		Price:         &price,
		Quantity:      "0.01",
	}

	payload, err := BuildNewOrderPayload(order, 99)
	if err != nil {
		t.Fatalf("BuildNewOrderPayload: %v", err)
	}

	var decoded matchingv1.OrderCommandEnvelope
	if err := proto.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	cmd := decoded.GetNewOrder()
	if cmd == nil {
		t.Fatal("expected new_order command")
	}
	if cmd.GetCommandId() != 99 {
		t.Fatalf("command_id=%d want 99 (outbox.id)", cmd.GetCommandId())
	}
	if cmd.GetOrder().GetOrderId() != 42 {
		t.Fatalf("order_id=%d want 42", cmd.GetOrder().GetOrderId())
	}
	if cmd.GetOrder().GetSymbol() != "BTC-USDT" {
		t.Fatalf("symbol=%q", cmd.GetOrder().GetSymbol())
	}
}
