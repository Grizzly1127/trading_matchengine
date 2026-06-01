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
	mode        string
	staticToken string
	issuers     *issuerRegistry
}

// NewVerifier 根据配置构造验签器。
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}
	v := &Verifier{
		mode:        cfg.Mode,
		staticToken: cfg.StaticToken,
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
		if token == v.staticToken {
			return Claims{
				Subject: "static-dev",
				Scopes:  append([]string(nil), AllScopes...),
				Method:  "static",
			}, nil
		}
		if v.mode == "static" {
			return Claims{}, fmt.Errorf("invalid static token")
		}
	}
	return v.issuers.validate(token)
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
