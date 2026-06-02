package authserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestHandleToken_Success(t *testing.T) {
	secretPath := writeTempSecret(t, []byte("auth-server-test-secret-key!!"))
	cfg := Config{
		Issuer:          "test-issuer",
		Audience:        []string{"trading-gateway"},
		HS256SecretFile: secretPath,
		TokenTTLSeconds: 600,
		Clients: []ClientConfig{{
			ClientID:     "web-bff",
			ClientSecret: "pass",
			Scopes:       []string{"orders:read", "push:connect"},
		}},
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(`{"client_id":"web-bff","client_secret":"pass"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.handleToken(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp tokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.AccessToken == "" || resp.TokenType != "Bearer" {
		t.Fatalf("resp=%+v", resp)
	}
}

func writeTempSecret(t *testing.T, b []byte) string {
	t.Helper()
	p := t.TempDir() + "/secret"
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
