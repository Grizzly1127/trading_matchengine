package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Verifier 校验 Bearer（static / JWT）。
type Verifier struct {
	mode                   string
	staticToken            string
	staticScopes           []string
	marketMakerStaticToken string
	marketMakerStaticScopes []string
	issuers                *issuerRegistry
}

// NewVerifier 根据配置构造验签器。
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	v := &Verifier{
		mode:                    cfg.Mode,
		staticToken:             cfg.StaticToken,
		staticScopes:            append([]string(nil), cfg.StaticScopes...),
		marketMakerStaticToken:  cfg.MarketMakerStaticToken,
		marketMakerStaticScopes: marketMakerStaticScopes(cfg),
	}
	if cfg.Mode == "jwt" || cfg.Mode == "static_or_jwt" {
		reg, err := newIssuerRegistry(ctx, cfg.JWT)
		if err != nil {
			return nil, err
		}
		v.issuers = reg
	}
	return v, nil
}

// Close 释放 JWKS 等资源。
func (v *Verifier) Close() {
	if v.issuers != nil {
		v.issuers.close()
	}
}

// VerifyBearer 校验 Authorization Bearer 并返回 Claims。
func (v *Verifier) VerifyBearer(_ context.Context, bearer string) (Claims, error) {
	token := strings.TrimSpace(bearer)
	if token == "" {
		return Claims{}, fmt.Errorf("empty token")
	}

	if v.mode == "static" || v.mode == "static_or_jwt" {
		if c, ok := v.verifyStaticToken(token); ok {
			return c, nil
		}
		if v.mode == "static" {
			return Claims{}, fmt.Errorf("invalid static token")
		}
	}
	return v.issuers.validate(token)
}

func marketMakerStaticScopes(cfg Config) []string {
	if len(cfg.MarketMakerStaticScopes) > 0 {
		return append([]string(nil), cfg.MarketMakerStaticScopes...)
	}
	if cfg.MarketMakerStaticToken == "" {
		return nil
	}
	return []string{
		ScopeMarketRead,
		ScopePushConnect,
		ScopePushTickerAll,
	}
}

func (v *Verifier) verifyStaticToken(token string) (Claims, bool) {
	if token == v.staticToken && v.staticToken != "" {
		scopes := append([]string(nil), AllScopes...)
		if len(v.staticScopes) > 0 {
			scopes = append([]string(nil), v.staticScopes...)
		}
		return Claims{Subject: "static-retail", Scopes: scopes, Method: "static"}, true
	}
	if token == v.marketMakerStaticToken && v.marketMakerStaticToken != "" {
		return Claims{
			Subject: "static-market-maker",
			Scopes:  append([]string(nil), v.marketMakerStaticScopes...),
			Method:  "static",
		}, true
	}
	return Claims{}, false
}

// SignHS256 使用本地 HS256 签发服务 JWT（cmd/auth 与测试用）。
func SignHS256(secret []byte, issuer, subject string, audience, scopes []string, ttl time.Duration) (string, error) {
	if len(secret) == 0 {
		return "", fmt.Errorf("empty secret")
	}
	now := time.Now().UTC()
	claims := ServiceClaims{
		Scope: strings.Join(scopes, " "),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   subject,
			Audience:  audience,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString(secret)
}
