package benchutil

import (
	"strings"
	"testing"

	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
)

func TestSubtractHistogram(t *testing.T) {
	pre := `
matching_processing_latency_ms_bucket{le="1"} 10
matching_processing_latency_ms_bucket{le="10"} 50
matching_processing_latency_ms_count 50
matching_processing_latency_ms_sum 200
`
	post := `
matching_processing_latency_ms_bucket{le="1"} 20
matching_processing_latency_ms_bucket{le="10"} 150
matching_processing_latency_ms_count 150
matching_processing_latency_ms_sum 900
`
	preH, err := ParseHistogram(strings.NewReader(pre), "matching_processing_latency_ms")
	if err != nil {
		t.Fatal(err)
	}
	postH, err := ParseHistogram(strings.NewReader(post), "matching_processing_latency_ms")
	if err != nil {
		t.Fatal(err)
	}
	delta, err := SubtractHistogram(postH, preH)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Count != 100 {
		t.Fatalf("delta count = %v, want 100", delta.Count)
	}
	p99 := delta.Quantile(0.99)
	if p99 < 9 || p99 > 10.5 {
		t.Fatalf("delta p99 = %v, want ~10", p99)
	}
}

func TestParseHistogramQuantile(t *testing.T) {
	body := `
matching_processing_latency_ms_bucket{le="1"} 10
matching_processing_latency_ms_bucket{le="5"} 50
matching_processing_latency_ms_bucket{le="10"} 100
matching_processing_latency_ms_count 100
matching_processing_latency_ms_sum 400
`
	h, err := ParseHistogram(strings.NewReader(body), "matching_processing_latency_ms")
	if err != nil {
		t.Fatal(err)
	}
	p99 := h.Quantile(0.99)
	if p99 < 9 || p99 > 10.5 {
		t.Fatalf("p99 = %v, want ~10", p99)
	}
}

func TestMarshalNewOrderEnvelope(t *testing.T) {
	b, err := MarshalNewOrderEnvelope(NewOrderParams{
		CommandID: 1, OrderID: 100, ClientOrderID: "b-1",
		Symbol: "BTC-USDT", Side: commonv1.Side_SIDE_BUY,
		Price: "65000", Quantity: "0.01",
	})
	if err != nil || len(b) == 0 {
		t.Fatalf("marshal: %v len=%d", err, len(b))
	}
}
