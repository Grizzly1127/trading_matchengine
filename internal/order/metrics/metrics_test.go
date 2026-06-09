package metrics

import "testing"

func TestMetricsSet(t *testing.T) {
	m := New()
	m.SetOutboxPendingCount(3)
	m.SetOrderStuckPendingSeconds(12.5)
	m.ObserveRelayBatchSize(100)
	m.ObserveRelayDispatchLatencyMs(25)
	m.AddRelayPublished(10)
	m.ObserveGRPCPlaceOrder(3)
	m.ObservePlaceValidate(1)
	m.ObservePlaceIdempotency(1)
	m.ObservePlaceDBTx(2)
}
