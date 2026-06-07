package metrics

import "testing"

func TestMetricsSet(t *testing.T) {
	m := New()
	m.SetOutboxPendingCount(3)
	m.SetOrderStuckPendingSeconds(12.5)
	m.ObserveRelayBatchSize(100)
	m.ObserveRelayDispatchLatencyMs(25)
	m.AddRelayPublished(10)
}
