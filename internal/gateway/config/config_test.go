package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPListen != ":8080" {
		t.Fatalf("http_listen=%q", cfg.HTTPListen)
	}
	if cfg.Auth.StaticToken == "" {
		t.Fatal("static_token empty")
	}
}
