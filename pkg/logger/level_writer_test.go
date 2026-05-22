package logger

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
)

func TestLevelSplitWriter_warnGoesToStderrStream(t *testing.T) {
	var stdout, stderr bytes.Buffer
	w := LevelSplitWriter{
		Stdout: &stdout,
		Stderr: &stderr,
		Dev:    false,
	}

	if _, err := w.WriteLevel(zerolog.InfoLevel, []byte("info-line\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteLevel(zerolog.WarnLevel, []byte("warn-line\n")); err != nil {
		t.Fatal(err)
	}

	if stdout.String() != "info-line\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.String() != "warn-line\n" {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
