package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

type fakeStore struct {
	entries    []Entry
	statusByID map[uint64]string
	published  []uint64
	retries    []uint64
	fetchErr   error
	markErr    error
}

func (f *fakeStore) isPublished(id uint64) bool {
	for _, p := range f.published {
		if p == id {
			return true
		}
	}
	return false
}

func (f *fakeStore) unpublishedEntries(limit int) []Entry {
	var out []Entry
	for _, e := range f.entries {
		if f.isPublished(e.ID) {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

type fakeClaim struct {
	store   *fakeStore
	entries []Entry
	pending []uint64
	done    bool
}

func (c *fakeClaim) Entries() []Entry {
	return c.entries
}

func (c *fakeClaim) MarkPublishedBatch(_ context.Context, ids []uint64) error {
	if c.done {
		return nil
	}
	if c.store.markErr != nil {
		return c.store.markErr
	}
	c.pending = append(c.pending, ids...)
	return nil
}

func (c *fakeClaim) Commit(context.Context) error {
	if c.done {
		return nil
	}
	c.done = true
	c.store.published = append(c.store.published, c.pending...)
	return nil
}

func (c *fakeClaim) Rollback(context.Context) error {
	if c.done {
		return nil
	}
	c.done = true
	c.pending = nil
	return nil
}

func (f *fakeStore) ClaimUnpublished(_ context.Context, limit int) (ClaimHandle, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	entries := f.unpublishedEntries(limit)
	if len(entries) == 0 {
		return EmptyClaim(), nil
	}
	return &fakeClaim{store: f, entries: entries}, nil
}

func (f *fakeStore) MarkPublished(_ context.Context, id uint64) error {
	return f.MarkPublishedBatch(context.Background(), []uint64{id})
}

func (f *fakeStore) MarkPublishedBatch(_ context.Context, ids []uint64) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.published = append(f.published, ids...)
	return nil
}

func (f *fakeStore) IncrementRetry(_ context.Context, id uint64) error {
	f.retries = append(f.retries, id)
	return nil
}

func (f *fakeStore) GetOrderStatus(_ context.Context, orderID uint64) (string, error) {
	m, err := f.GetOrderStatusesBatch(context.Background(), []uint64{orderID})
	if err != nil {
		return "", err
	}
	if s, ok := m[orderID]; ok {
		return s, nil
	}
	return "PENDING", nil
}

func (f *fakeStore) GetOrderStatusesBatch(_ context.Context, orderIDs []uint64) (map[uint64]string, error) {
	out := make(map[uint64]string, len(orderIDs))
	for _, id := range orderIDs {
		if s, ok := f.statusByID[id]; ok {
			out[id] = s
		} else {
			out[id] = "PENDING"
		}
	}
	return out, nil
}

type fakeWriter struct {
	writes      []writeCall
	batchWrites []batchWriteCall
	err         error
}

type writeCall struct {
	topic     string
	partition int
	key       string
	value     []byte
}

type batchWriteCall struct {
	topic     string
	partition int
	key       string
	values    [][]byte
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

func (f *fakeWriter) WriteBatchAt(_ context.Context, topic string, partition int, key []byte, values [][]byte) error {
	if f.err != nil {
		return f.err
	}
	copied := make([][]byte, len(values))
	for i, v := range values {
		copied[i] = append([]byte(nil), v...)
	}
	f.batchWrites = append(f.batchWrites, batchWriteCall{
		topic:     topic,
		partition: partition,
		key:       string(key),
		values:    copied,
	})
	return nil
}

func testLog() zerolog.Logger {
	return zerolog.Nop()
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
	if len(writer.batchWrites) != 1 {
		t.Fatalf("batchWrites=%d want 1", len(writer.batchWrites))
	}
	bw := writer.batchWrites[0]
	if bw.topic != "order.commands" || bw.partition != 0 || len(bw.values) != 1 {
		t.Fatalf("unexpected batch write: %+v", bw)
	}
	if len(store.published) != 1 || store.published[0] != 1 {
		t.Fatalf("published=%v want [1]", store.published)
	}
}

func TestRelay_DispatchBatchMultiple(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{
			{ID: 1, AggregateID: 10, Payload: []byte("a"), Topic: "order.commands", PartitionKey: "BTC-USDT"},
			{ID: 2, AggregateID: 11, Payload: []byte("b"), Topic: "order.commands", PartitionKey: "BTC-USDT"},
		},
		statusByID: map[uint64]string{10: "PENDING", 11: "PENDING"},
	}
	writer := &fakeWriter{}
	relay := &Relay{Store: store, Writer: writer, Config: RelayConfig{Partition: 0, MaxRetry: 3}}

	claim, err := store.ClaimUnpublished(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.dispatchBatch(context.Background(), relay.normalizedConfig(), claim, claim.Entries(), testLog()); err != nil {
		t.Fatal(err)
	}
	if err := claim.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(writer.batchWrites) != 1 || len(writer.batchWrites[0].values) != 2 {
		t.Fatalf("batchWrites=%+v want 1 batch with 2 values", writer.batchWrites)
	}
	if len(store.published) != 2 {
		t.Fatalf("published=%v want 2 ids", store.published)
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
	if len(writer.batchWrites) != 0 {
		t.Fatal("expected no kafka write for terminal order")
	}
	if len(store.published) != 1 {
		t.Fatalf("published=%v want mark skipped", store.published)
	}
}

func TestRelay_KafkaErrorIncrementsRetry(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{{
			ID:           3,
			AggregateID:  12,
			Topic:        "order.commands",
			Payload:      []byte("x"),
			PartitionKey: "BTC-USDT",
		}},
		statusByID: map[uint64]string{12: "PENDING"},
	}
	writer := &fakeWriter{err: errors.New("kafka down")}
	relay := &Relay{Store: store, Writer: writer, Config: RelayConfig{MaxRetry: 3}}

	claim, err := store.ClaimUnpublished(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := relay.dispatchBatch(context.Background(), relay.normalizedConfig(), claim, claim.Entries(), testLog()); err != nil {
		t.Fatalf("dispatchBatch: %v", err)
	}
	_ = claim.Rollback(context.Background())
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
	if len(writer.batchWrites) != 1 || writer.batchWrites[0].partition != 3 {
		t.Fatalf("partition=%d want 3", writer.batchWrites[0].partition)
	}
}

func TestRelay_pollOnceReturnsBatchSizeForContinuousPoll(t *testing.T) {
	store := &fakeStore{
		entries: []Entry{
			{ID: 1, AggregateID: 10, Payload: []byte("a"), Topic: "t", PartitionKey: "BTC-USDT"},
		},
		statusByID: map[uint64]string{10: "PENDING"},
	}
	writer := &fakeWriter{}
	relay := &Relay{
		Store:  store,
		Writer: writer,
		Config: RelayConfig{BatchSize: 1, MaxRetry: 3},
	}
	if n := relay.pollOnce(context.Background(), relay.normalizedConfig(), testLog()); n != 1 {
		t.Fatalf("pollOnce returned %d want 1", n)
	}
}

func TestRelay_normalizedConfigWorkers(t *testing.T) {
	r := &Relay{Config: RelayConfig{Workers: 4}}
	if got := r.normalizedConfig().Workers; got != 4 {
		t.Fatalf("workers=%d want 4", got)
	}
	r2 := &Relay{Config: RelayConfig{}}
	if got := r2.normalizedConfig().Workers; got != 1 {
		t.Fatalf("default workers=%d want 1", got)
	}
}
