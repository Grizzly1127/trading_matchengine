package wal

import (
	"errors"
	"io"
	"os"
	"testing"
)

func TestFileWriter_groupCommitDeferredSync(t *testing.T) {
	dir := t.TempDir()
	cfg := FileWriterConfig{SyncEveryRecords: 4}
	w, err := OpenFileWriter(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 3; i++ {
		if _, err := w.AppendNext(EventTypeNewOrder, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if w.LastSeq() != 3 || w.LastDurableSeq() != 0 {
		t.Fatalf("seq=%d durable=%d want 3/0 before sync", w.LastSeq(), w.LastDurableSeq())
	}

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if w.LastDurableSeq() != 3 {
		t.Fatalf("durable=%d want 3", w.LastDurableSeq())
	}

	paths, err := SegmentPaths(dir)
	if err != nil || len(paths) != 1 {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRecord(raw)
	if err != nil || got.SeqID != 1 {
		t.Fatalf("first record: %+v err=%v", got, err)
	}

	if _, err := w.AppendNext(EventTypeNewOrder, []byte("y")); err != nil {
		t.Fatal(err)
	}
	if w.LastDurableSeq() != 3 {
		t.Fatalf("durable=%d want 3 before second sync", w.LastDurableSeq())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenFileWriter(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.LastSeq() != 4 {
		t.Fatalf("reopen lastSeq=%d want 4", w2.LastSeq())
	}
}

func TestFileWriter_groupCommitReopenReadsAllSynced(t *testing.T) {
	dir := t.TempDir()
	cfg := FileWriterConfig{SyncEveryRecords: 8}
	w, err := OpenFileWriter(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := w.AppendNext(EventTypeNewOrder, []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	paths, err := SegmentPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var count int
	for {
		if _, err := ReadRecord(f); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		count++
	}
	if count != 5 {
		t.Fatalf("records=%d want 5", count)
	}
}
