package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileReader_readAll(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(i, EventTypeNewOrder, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}

	records, err := r.ReadAll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("len = %d, want 3", len(records))
	}
	if records[0].SeqID != 1 || records[2].SeqID != 3 {
		t.Fatalf("records = %+v", records)
	}
}

func TestFileReader_readFrom_skipsApplied(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append(i, EventTypeNewOrder, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}

	records, err := r.ReadAll(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("len = %d, want 2", len(records))
	}
	if records[0].SeqID != 4 || records[1].SeqID != 5 {
		t.Fatalf("records = %+v", records)
	}
}

func TestFileReader_readFrom_emptyWhenCaughtUp(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(1, EventTypeNewOrder, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := r.ReadAll(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("len = %d, want 0", len(records))
	}
}

func TestFileReader_readAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{MaxSegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("0123456789")
	for i := uint64(1); i <= 4; i++ {
		if err := w.Append(i, EventTypeNewOrder, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := r.ReadAll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("len = %d, want 4", len(records))
	}
}

func TestFileReader_emptyDir(t *testing.T) {
	dir := t.TempDir()
	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := r.ReadAll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("len = %d, want 0", len(records))
	}
}

func TestFileReader_missingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAll(0); err != nil {
		t.Fatal(err)
	}
}

func TestIterator_corruptRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_000001.log")
	if err := os.WriteFile(path, []byte("bad-data"), 0o644); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.ReadAll(0)
	if err == nil {
		t.Fatal("expected error for corrupt wal")
	}
}

func TestIterator_manual(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(10, EventTypeCancelOrder, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenFileReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	it, err := r.ReadFrom(9)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	if !it.Next() {
		t.Fatal("expected record")
	}
	rec := it.Record()
	if rec.SeqID != 10 || rec.EventType != EventTypeCancelOrder {
		t.Fatalf("rec = %+v", rec)
	}
	if it.Next() {
		t.Fatal("expected no more records")
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
}
