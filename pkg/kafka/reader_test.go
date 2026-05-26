package kafka

import (
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestPlanReaderStart_fromLatest(t *testing.T) {
	g, start, seek := planReaderStart(ReaderConfig{
		GroupID:     "matching-shard-0",
		StartOffset: -1,
	})
	if g != "matching-shard-0" || start != kafkago.LastOffset || seek != -1 {
		t.Fatalf("got group=%q start=%d seek=%d", g, start, seek)
	}
}

func TestPlanReaderStart_walResume(t *testing.T) {
	g, start, seek := planReaderStart(ReaderConfig{
		GroupID:     "matching-shard-0",
		StartOffset: 9,
	})
	if g != "" || start != kafkago.FirstOffset || seek != 9 {
		t.Fatalf("got group=%q start=%d seek=%d", g, start, seek)
	}
}

func TestPlanReaderStart_fromEarliest(t *testing.T) {
	g, start, seek := planReaderStart(ReaderConfig{
		GroupID:     "order-service",
		StartOffset: 0,
	})
	if g != "order-service" || start != 0 || seek != -1 {
		t.Fatalf("got group=%q start=%d seek=%d", g, start, seek)
	}
}
