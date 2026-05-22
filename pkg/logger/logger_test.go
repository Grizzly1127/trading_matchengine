package logger

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

func TestNew_setsServiceField(t *testing.T) {
	var buf bytes.Buffer
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	l := zerolog.New(&buf).With().
		Timestamp().
		Str("service", "testsvc").
		Logger()

	l.Info().Msg("hello")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	if m["service"] != "testsvc" {
		t.Fatalf("service = %v, want testsvc", m["service"])
	}
}
