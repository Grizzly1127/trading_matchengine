package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Grizzly1127/trading_matchengine/pkg/shardmgr"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

func TestPlaceOrder_blockedDuringMigration(t *testing.T) {
	mgr, err := shardmgr.NewFromConfig(shardmgr.FileConfig{
		Shards: []shardmgr.ShardDef{{
			ShardID: "shard-0", KafkaPartition: 0, Node: "n0",
			Tier: shardmgr.TierShared, Symbols: []string{"BTC-USDT"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.StartMigration("BTC-USDT", shardmgr.Route{
		ShardID: "shard-1", KafkaPartition: 1, Node: "n1", Tier: shardmgr.TierExclusive,
	}); err != nil {
		t.Fatal(err)
	}

	svc := &OrderService{Shards: mgr}
	_, err = svc.validatePlaceOrder(context.Background(), &orderv1.PlaceOrderRequest{
		UserId: 1, ClientOrderId: "c1", Symbol: "BTC-USDT",
		Side: commonv1.Side_SIDE_BUY, Type: commonv1.OrderType_ORDER_TYPE_LIMIT,
		Price: &commonv1.Decimal{Value: "100"}, Quantity: &commonv1.Decimal{Value: "0.01"},
	})
	if !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("want failed precondition, got %v", err)
	}
}
