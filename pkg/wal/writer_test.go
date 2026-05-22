package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileWriter_appendAndReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(1, EventTypeNewOrder, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(2, EventTypeCancelOrder, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if w2.LastSeq() != 2 {
		t.Fatalf("lastSeq = %d, want 2", w2.LastSeq())
	}
	if err := w2.Append(3, EventTypeNewOrder, []byte("c")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}

	paths, err := SegmentPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("segments = %d, want 1", len(paths))
	}

	var count int
	f, err := os.Open(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for {
		if _, err := ReadRecord(f); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("records = %d, want 3", count)
	}
}

func TestFileWriter_nonMonotonicSeq(t *testing.T) {
	w, err := OpenFileWriter(t.TempDir(), FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(5, EventTypeNewOrder, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(5, EventTypeNewOrder, nil); !errors.Is(err, ErrNonMonotonic) {
		t.Fatalf("err = %v, want %v", err, ErrNonMonotonic)
	}
}

func TestFileWriter_rotateSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{MaxSegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte("0123456789")
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append(i, EventTypeNewOrder, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	paths, err := SegmentPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 2 {
		t.Fatalf("segments = %d, want >= 2", len(paths))
	}
}

func TestFileWriter_appendAfterClose(t *testing.T) {
	w, err := OpenFileWriter(t.TempDir(), FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(1, EventTypeNewOrder, nil); err != ErrWriterClosed {
		t.Fatalf("err = %v, want %v", err, ErrWriterClosed)
	}
}

func TestFileWriter_fsyncVisibleBeforeClose(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Unix(123, 0).UTC()
	rec := NewRecord(1, EventTypeNewOrder, []byte("persist"), ts)
	if err := w.Append(rec.SeqID, rec.EventType, rec.Payload); err != nil {
		t.Fatal(err)
	}

	paths, err := SegmentPaths(dir)
	if err != nil || len(paths) == 0 {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.SeqID != 1 || string(got.Payload) != "persist" {
		t.Fatalf("got = %+v", got)
	}
	_ = w.Close()
}

func TestSegmentPaths_sorted(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"wal_000002.log", "wal_000001.log", "wal_000003.log"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := SegmentPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 || filepath.Base(paths[0]) != "wal_000001.log" {
		t.Fatalf("paths = %v", paths)
	}
}
