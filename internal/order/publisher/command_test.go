package publisher

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
)

func TestBuildNewOrderEnvelope(t *testing.T) {
	price := "65000.5"
	order := &repository.Order{
		ID:            42,
		ClientOrderID: "demo-001",
		Symbol:        "BTC-USDT",
		Side:          1,
		OrderType:     1,
		Price:         &price,
		Quantity:      "0.01",
		Status:        "PENDING",
	}

	env := buildNewOrderEnvelope(order)
	b, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded matchingv1.OrderCommandEnvelope
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	cmd := decoded.GetNewOrder()
	if cmd == nil {
		t.Fatal("expected new_order command")
	}
	if cmd.GetCommandId() != 42 {
		t.Fatalf("command_id=%d want 42", cmd.GetCommandId())
	}
	if cmd.GetOrder().GetOrderId() != 42 {
		t.Fatalf("order_id=%d want 42", cmd.GetOrder().GetOrderId())
	}
	if cmd.GetOrder().GetSymbol() != "BTC-USDT" {
		t.Fatalf("symbol=%q", cmd.GetOrder().GetSymbol())
	}
}
