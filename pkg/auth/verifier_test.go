package auth

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestVerifier_StaticMode(t *testing.T) {
	v, err := NewVerifier(context.Background(), Config{
		Mode:        "static",
		StaticToken: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	c, err := v.VerifyBearer(context.Background(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if !HasScopes(c, ScopeOrdersWrite, ScopeBalancesAdmin) {
		t.Fatalf("scopes=%v", c.Scopes)
	}
}

func TestVerifier_StaticTierIsolation(t *testing.T) {
	v, err := NewVerifier(context.Background(), Config{
		Mode:                   "static",
		StaticToken:            "retail",
		StaticScopes:           []string{ScopePushConnect, ScopeMarketRead},
		MarketMakerStaticToken: "mm",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	retail, err := v.VerifyBearer(context.Background(), "retail")
	if err != nil {
		t.Fatal(err)
	}
	if HasScopes(retail, ScopePushTickerAll) {
		t.Fatalf("retail scopes=%v", retail.Scopes)
	}

	mm, err := v.VerifyBearer(context.Background(), "mm")
	if err != nil {
		t.Fatal(err)
	}
	if !HasScopes(mm, ScopePushTickerAll) || !HasScopes(mm, ScopePushConnect) {
		t.Fatalf("mm scopes=%v", mm.Scopes)
	}
}

func TestVerifier_HS256JWT(t *testing.T) {
	secret := []byte("test-hs256-secret-key")
	issuer := "trading-matchengine-dev"
	aud := []string{"trading-gateway"}
	scope := []string{ScopeOrdersRead, ScopePushConnect}

	token, err := SignHS256(secret, issuer, "web-bff", aud, scope, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	tmp := t.TempDir() + "/hs256.secret"
	if err := os.WriteFile(tmp, secret, 0o600); err != nil {
		t.Fatal(err)
	}

	v, err := NewVerifier(context.Background(), Config{
		Mode: "jwt",
		JWT: JWTConfig{
			Audience: aud,
			Issuers: []IssuerConfig{{
				Issuer:          issuer,
				HS256SecretFile: tmp,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	c, err := v.VerifyBearer(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if c.Subject != "web-bff" || !HasScopes(c, ScopeOrdersRead) || HasScopes(c, ScopeBalancesAdmin) {
		t.Fatalf("claims=%+v", c)
	}
}
