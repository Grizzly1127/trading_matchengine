package wal

import (
	"testing"
)

// BenchmarkFileWriter_appendFsync 测量单条 WAL Append+fsync（L1 组件基准）。
func BenchmarkFileWriter_appendFsync(b *testing.B) {
	dir := b.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	payload := []byte("bench-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(uint64(i+1), EventTypeNewOrder, payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileWriter_groupCommit32 每 32 条一次 Sync（组提交）。
func BenchmarkFileWriter_groupCommit32(b *testing.B) {
	dir := b.TempDir()
	w, err := OpenFileWriter(dir, FileWriterConfig{SyncEveryRecords: 32})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	payload := []byte("bench-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.AppendNext(EventTypeNewOrder, payload); err != nil {
			b.Fatal(err)
		}
		if (i+1)%32 == 0 {
			if err := w.Sync(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
