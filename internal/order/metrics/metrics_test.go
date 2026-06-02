package metrics

import "testing"

func TestMetricsSet(t *testing.T) {
	m := New()
	m.SetOutboxPendingCount(3)
	m.SetOrderStuckPendingSeconds(12.5)
}
