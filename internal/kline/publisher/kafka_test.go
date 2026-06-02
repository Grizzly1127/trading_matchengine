package publisher

import (
	"testing"

	klinev1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/kline/v1"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestKlineClosedEventRoundTrip(t *testing.T) {
	msg := &klinev1.KlineClosedEvent{
		Symbol:      "BTC-USDT",
		Interval:    "1m",
		OpenTimeMs:  1716191940000,
		CloseTimeMs: 1716191999999,
		Open:        &commonv1.Decimal{Value: "100"},
		High:        &commonv1.Decimal{Value: "110"},
		Low:         &commonv1.Decimal{Value: "90"},
		Close:       &commonv1.Decimal{Value: "105"},
		Volume:      &commonv1.Decimal{Value: "12.5"},
		TsMs:        1716191987500,
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded klinev1.KlineClosedEvent
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetSymbol() != "BTC-USDT" || decoded.GetInterval() != "1m" {
		t.Fatalf("decoded=%+v", &decoded)
	}
}

func TestKafkaPublisherRequiresWriter(t *testing.T) {
	p := NewKafkaPublisher(nil, "kline.raw")
	if p == nil {
		t.Fatal("nil publisher")
	}
}
