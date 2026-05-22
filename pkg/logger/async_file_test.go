package logger

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAsyncWriter_writesAndCloses(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewAsyncWriter(&buf, nil, 32)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello\n" {
		t.Fatalf("buf = %q", buf.String())
	}
}

func TestAsyncWriter_withLumberjack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	sink, err := openFileSink(path, RotateConfig{
		MaxSizeMB:   1,
		MaxAgeDays:  7,
		RotateDaily: false,
		LocalTime:   true,
	})
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewAsyncWriter(sink.w, &sinkCloser{sink.close}, 32)
	if err != nil {
		t.Fatal(err)
	}

	line := `{"message":"hello"}` + "\n"
	if _, err := w.Write([]byte(line)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		b, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(b), "hello") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for async write")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
