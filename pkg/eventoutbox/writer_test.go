package eventoutbox_test

import (
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/eventoutbox"
)

func TestRecordRoundTrip(t *testing.T) {
	want := eventoutbox.NewRecord(7, 99, eventoutbox.TopicMatch, 0, 42, "BTC-USDT", []byte("hello-event"), time.Unix(100, 0))

	frame, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := eventoutbox.DecodeRecord(frame)
	if err != nil {
		t.Fatal(err)
	}
	if got.OutboxSeq != want.OutboxSeq || got.WalSeq != want.WalSeq || got.TopicID != want.TopicID {
		t.Fatalf("header mismatch: %+v vs %+v", got, want)
	}
	if got.PartitionKey != want.PartitionKey || string(got.Payload) != string(want.Payload) {
		t.Fatalf("body mismatch: %+v", got)
	}
}

func TestFileWriterAppendSync(t *testing.T) {
	dir := t.TempDir()
	w, err := eventoutbox.OpenFileWriter(dir, eventoutbox.FileWriterConfig{SyncEveryRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	rec := eventoutbox.NewRecord(0, 1, eventoutbox.TopicTrade, 0, 10, "ETH-USDT", []byte("x"), time.Time{})
	if _, err := w.AppendNext(rec); err != nil {
		t.Fatal(err)
	}
	if w.LastDurableSeq() != 1 {
		t.Fatalf("durable seq=%d want 1", w.LastDurableSeq())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := eventoutbox.FetchUnpublished(dir, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].OutboxSeq != 1 {
		t.Fatalf("fetch: %+v", got)
	}

	meta := eventoutbox.Meta{LastPublishedSeq: 1}
	if err := eventoutbox.SaveMeta(dir, meta); err != nil {
		t.Fatal(err)
	}
	got2, err := eventoutbox.FetchUnpublished(dir, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 0 {
		t.Fatalf("expected no unpublished, got %d", len(got2))
	}
}

func TestFileWriterGroupCommitRequiresSync(t *testing.T) {
	dir := t.TempDir()
	w, err := eventoutbox.OpenFileWriter(dir, eventoutbox.FileWriterConfig{SyncEveryRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	rec := eventoutbox.NewRecord(0, 1, eventoutbox.TopicMatch, 0, 1, "BTC-USDT", []byte("a"), time.Time{})
	if _, err := w.AppendNext(rec); err != nil {
		t.Fatal(err)
	}
	if w.LastDurableSeq() != 0 {
		t.Fatal("group commit should not auto sync")
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if w.LastDurableSeq() != 1 {
		t.Fatalf("after sync durable=%d", w.LastDurableSeq())
	}
	_ = w.Close()
}
