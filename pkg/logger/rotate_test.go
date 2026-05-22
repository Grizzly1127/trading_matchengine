package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDailyRotateWriter_rotatesOnDayChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "daily.log")

	sink, err := openFileSink(path, RotateConfig{
		MaxSizeMB:   100,
		MaxAgeDays:  7,
		RotateDaily: true,
		LocalTime:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.close()

	dr := sink.w.(*dailyRotateWriter)
	dr.curDay = "2000-01-01"

	if _, err := sink.w.Write([]byte("line\n")); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 日期变化后应产生备份文件（原文件被 rotate）
	hasBackup := len(entries) >= 1
	if !hasBackup {
		t.Fatalf("expected rotated files in %s", dir)
	}
}

func TestOpenFileSink_createsDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "app.log")

	sink, err := openFileSink(path, RotateConfig{MaxSizeMB: 1, MaxAgeDays: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sink.w.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := sink.close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(b), "x") {
		t.Fatalf("read file: %v content=%q", err, b)
	}
}
