package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
)

func TestRequireScopes_Forbidden(t *testing.T) {
	secret := []byte("scope-test-secret-key-32bytes!!")
	tmp := t.TempDir() + "/hs256.secret"
	if err := os.WriteFile(tmp, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	v, err := auth.NewVerifier(context.Background(), auth.Config{
		Mode: "jwt",
		JWT: auth.JWTConfig{
			Audience: []string{"trading-gateway"},
			Issuers: []auth.IssuerConfig{{
				Issuer:          "test-issuer",
				HS256SecretFile: tmp,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	token, err := auth.SignHS256(secret, "test-issuer", "readonly", []string{"trading-gateway"},
		[]string{auth.ScopeOrdersRead}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	h := Authenticate(v)(RequireScopes(auth.ScopeBalancesAdmin)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("should not reach")
	})))

	req := httptest.NewRequest(http.MethodPost, "/v1/balances", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
