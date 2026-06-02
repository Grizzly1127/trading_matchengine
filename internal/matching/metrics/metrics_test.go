package metrics

import (
	"testing"
	"time"
)

func TestMetricsObserve(t *testing.T) {
	m := New()
	m.ObserveProcessing(2 * time.Millisecond)
	m.ObserveWalAppend(time.Millisecond)
	m.SetLastProcessedOffset(42)
	m.SetKafkaLag(3)
	m.SetWalLastSeq(7)

	s := m.Snap()
	if s.CommandsProcessed != 1 {
		t.Fatalf("commands_processed = %d, want 1", s.CommandsProcessed)
	}
	if s.LastProcessedOffset != 42 {
		t.Fatalf("last_offset = %d, want 42", s.LastProcessedOffset)
	}
	if s.KafkaLag != 3 {
		t.Fatalf("kafka_lag = %d, want 3", s.KafkaLag)
	}
	if s.WalLastSeq != 7 {
		t.Fatalf("wal_last_seq = %d, want 7", s.WalLastSeq)
	}
}
