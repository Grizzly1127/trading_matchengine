package auth

import (
	"context"
	"fmt"
	"sync"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type issuerValidator struct {
	issuer   string
	audience []string
	hsSecret []byte
	jwks     keyfunc.Keyfunc
}

func newIssuerValidator(ctx context.Context, iss IssuerConfig, audience []string) (*issuerValidator, error) {
	v := &issuerValidator{
		issuer:   iss.Issuer,
		audience: audience,
	}
	if iss.HS256SecretFile != "" {
		secret, err := ReadSecretFile(iss.HS256SecretFile)
		if err != nil {
			return nil, err
		}
		v.hsSecret = secret
		return v, nil
	}
	k, err := keyfunc.NewDefaultCtx(ctx, []string{iss.JWKSURL})
	if err != nil {
		return nil, fmt.Errorf("jwks %q: %w", iss.JWKSURL, err)
	}
	v.jwks = k
	return v, nil
}

func (v *issuerValidator) validate(token string) (Claims, error) {
	var claims ServiceClaims
	keyFunc := v.keyFunc()
	parsed, err := jwt.ParseWithClaims(token, &claims, keyFunc,
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience...),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg(), jwt.SigningMethodRS256.Alg(), jwt.SigningMethodES256.Alg()}),
	)
	if err != nil {
		return Claims{}, err
	}
	if !parsed.Valid {
		return Claims{}, fmt.Errorf("invalid token")
	}
	sub, _ := parsed.Claims.GetSubject()
	scopes := ParseScopeClaim(claims.Scope)
	if len(scopes) == 0 {
		if raw, ok := parsed.Claims.(jwt.MapClaims); ok {
			scopes = ParseScopeClaim(raw["scope"])
		}
	}
	if sub == "" {
		return Claims{}, fmt.Errorf("missing sub")
	}
	if len(scopes) == 0 {
		return Claims{}, fmt.Errorf("missing scope")
	}
	return Claims{Subject: sub, Scopes: scopes, Method: "jwt"}, nil
}

func (v *issuerValidator) keyFunc() jwt.Keyfunc {
	if len(v.hsSecret) > 0 {
		return func(t *jwt.Token) (any, error) {
			if t.Method != jwt.SigningMethodHS256 {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return v.hsSecret, nil
		}
	}
	return v.jwks.Keyfunc
}

func (v *issuerValidator) close() {
	// JWKS 由 keyfunc 内部 goroutine 刷新；进程退出时随 Go runtime 回收。
}

type issuerRegistry struct {
	mu        sync.Mutex
	byIssuer  map[string]*issuerValidator
	audience  []string
	issuerCfg []IssuerConfig
}

func newIssuerRegistry(ctx context.Context, cfg JWTConfig) (*issuerRegistry, error) {
	r := &issuerRegistry{
		byIssuer:  make(map[string]*issuerValidator, len(cfg.Issuers)),
		audience:  cfg.Audience,
		issuerCfg: cfg.Issuers,
	}
	for _, iss := range cfg.Issuers {
		v, err := newIssuerValidator(ctx, iss, cfg.Audience)
		if err != nil {
			r.close()
			return nil, err
		}
		r.byIssuer[iss.Issuer] = v
	}
	return r, nil
}

func (r *issuerRegistry) validate(token string) (Claims, error) {
	unverified, _, err := jwt.NewParser().ParseUnverified(token, &ServiceClaims{})
	if err != nil {
		return Claims{}, err
	}
	iss, err := unverified.Claims.GetIssuer()
	if err != nil || iss == "" {
		return Claims{}, fmt.Errorf("missing iss")
	}
	r.mu.Lock()
	v, ok := r.byIssuer[iss]
	r.mu.Unlock()
	if !ok {
		return Claims{}, fmt.Errorf("unknown issuer %q", iss)
	}
	return v.validate(token)
}

func (r *issuerRegistry) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range r.byIssuer {
		v.close()
	}
}
