package outbox

import (
	"context"
	"errors"
	"testing"
)

type fakeStore struct {
	entries    []Entry
	statusByID map[uint64]string
	published  []uint64
	retries    []uint64
	fetchErr   error
	markErr    error
}

func (f *fakeStore) FetchUnpublished(_ context.Context, limit int) ([]Entry, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	if limit <= 0 || limit >= len(f.entries) {
		return append([]Entry(nil), f.entries...), nil
	}
	return append([]Entry(nil), f.entries[:limit]...), nil
}

func (f *fakeStore) MarkPublished(_ context.Context, id uint64) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.published = append(f.published, id)
	return nil
}

func (f *fakeStore) IncrementRetry(_ context.Context, id uint64) error {
	f.retries = append(f.retries, id)
	return nil
}

func (f *fakeStore) GetOrderStatus(_ context.Context, orderID uint64) (string, error) {
	if s, ok := f.statusByID[orderID]; ok {
		return s, nil
	}
	return "PENDING", nil
}

type fakeWriter struct {
	writes []writeCall
	err    error
}

type writeCall struct {
	topic     string
	partition int
	key       string
	value     []byte
}

func (f *fakeWriter) WriteAt(_ context.Context, topic string, partition int, key, value []byte) error {
	if f.err != nil {
		return f.err
	}
	f.writes = append(f.writes, writeCall{
		topic:     topic,
		partition: partition,
		key:       string(key),
		value:     append([]byte(nil), value...),
	})
	return nil
}

func TestRelay_DispatchPending(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{{
			ID:           1,
			AggregateID:  10,
			EventType:    EventTypeNewOrder,
			Payload:      []byte("payload"),
			Topic:        "order.commands",
			PartitionKey: "BTC-USDT",
		}},
		statusByID: map[uint64]string{10: "PENDING"},
	}
	writer := &fakeWriter{}
	relay := &Relay{
		Store:  store,
		Writer: writer,
		Config: RelayConfig{Partition: 0, MaxRetry: 3},
	}

	if err := relay.dispatchOne(context.Background(), relay.normalizedConfig(), store.entries[0]); err != nil {
		t.Fatalf("dispatchOne: %v", err)
	}
	if len(writer.writes) != 1 {
		t.Fatalf("writes=%d want 1", len(writer.writes))
	}
	if writer.writes[0].topic != "order.commands" || writer.writes[0].partition != 0 {
		t.Fatalf("unexpected write: %+v", writer.writes[0])
	}
	if len(store.published) != 1 || store.published[0] != 1 {
		t.Fatalf("published=%v want [1]", store.published)
	}
}

func TestRelay_SkipTerminalOrder(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{{
			ID:          2,
			AggregateID: 11,
			Topic:       "order.commands",
		}},
		statusByID: map[uint64]string{11: "CANCELED"},
	}
	writer := &fakeWriter{}
	relay := &Relay{Store: store, Writer: writer, Config: RelayConfig{MaxRetry: 3}}

	if err := relay.dispatchOne(context.Background(), relay.normalizedConfig(), store.entries[0]); err != nil {
		t.Fatalf("dispatchOne: %v", err)
	}
	if len(writer.writes) != 0 {
		t.Fatal("expected no kafka write for terminal order")
	}
	if len(store.published) != 1 {
		t.Fatalf("published=%v want mark skipped", store.published)
	}
}

func TestRelay_KafkaErrorIncrementsRetry(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{{
			ID:          3,
			AggregateID: 12,
			Topic:       "order.commands",
			Payload:     []byte("x"),
		}},
		statusByID: map[uint64]string{12: "PENDING"},
	}
	writer := &fakeWriter{err: errors.New("kafka down")}
	relay := &Relay{Store: store, Writer: writer, Config: RelayConfig{MaxRetry: 3}}

	if err := relay.dispatchOne(context.Background(), relay.normalizedConfig(), store.entries[0]); err == nil {
		t.Fatal("expected error")
	}
	if len(store.retries) != 1 || store.retries[0] != 3 {
		t.Fatalf("retries=%v want [3]", store.retries)
	}
	if len(store.published) != 0 {
		t.Fatal("should not mark published on kafka failure")
	}
}

type fakeResolver struct {
	partition int
	err       error
}

func (f *fakeResolver) PartitionForSymbol(string) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.partition, nil
}

func TestRelay_dispatch_resolvesPartitionBySymbol(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{{
			ID: 1, AggregateID: 10, EventType: EventTypeNewOrder,
			Payload: []byte("x"), Topic: "order.commands",
			PartitionKey: "ETH-USDT",
		}},
		statusByID: map[uint64]string{10: "PENDING"},
	}
	writer := &fakeWriter{}
	relay := &Relay{
		Store:  store,
		Writer: writer,
		Config: RelayConfig{Partition: 0, MaxRetry: 3, Resolver: &fakeResolver{partition: 3}},
	}
	if err := relay.dispatchOne(context.Background(), relay.normalizedConfig(), store.entries[0]); err != nil {
		t.Fatal(err)
	}
	if len(writer.writes) != 1 || writer.writes[0].partition != 3 {
		t.Fatalf("partition=%d want 3", writer.writes[0].partition)
	}
}
