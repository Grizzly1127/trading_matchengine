package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNew_consoleAndAsyncFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "matching.log")

	res, err := New(Config{
		Service:    "matching",
		Level:      "info",
		Dev:        false,
		File:       logPath,
		Async:      true,
		BufferSize: 16,
		Rotate: RotateConfig{
			MaxSizeMB:   1,
			MaxAgeDays:  7,
			RotateDaily: false,
			LocalTime:   true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	res.Logger.Info().Msg("disk-test")

	deadline := time.Now().Add(2 * time.Second)
	for {
		b, err := os.ReadFile(logPath)
		if err == nil && strings.Contains(string(b), "disk-test") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("log file missing expected content")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
