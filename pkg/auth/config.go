package auth

import (
	"fmt"
	"os"
	"strings"
)

// Config 鉴权配置（Gateway / Push / cmd/auth 共用 JSON 形态）。
type Config struct {
	// Mode: static | jwt | static_or_jwt
	Mode        string `json:"mode"`
	StaticToken string `json:"static_token"`
	JWT         JWTConfig `json:"jwt"`
}

// JWTConfig JWT 验签参数。
type JWTConfig struct {
	Audience []string       `json:"audience"`
	Issuers  []IssuerConfig `json:"issuers"`
}

// IssuerConfig 单个 issuer（外部 JWKS 或本地 HS256）。
type IssuerConfig struct {
	Issuer          string `json:"issuer"`
	JWKSURL         string `json:"jwks_url"`
	HS256SecretFile string `json:"hs256_secret_file"`
}

// TLSConfig 服务端 mTLS（可选）。
type TLSConfig struct {
	Enabled           bool   `json:"enabled"`
	CertFile          string `json:"cert_file"`
	KeyFile           string `json:"key_file"`
	ClientCAFile      string `json:"client_ca_file"`
	RequireClientCert bool   `json:"require_client_cert"`
}

// Normalize 填充默认值并校验。
func (c *Config) Normalize() error {
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		c.Mode = "static"
	}
	switch c.Mode {
	case "static", "jwt", "static_or_jwt":
	default:
		return fmt.Errorf("auth: unsupported mode %q", c.Mode)
	}
	if c.Mode == "static" || c.Mode == "static_or_jwt" {
		if strings.TrimSpace(c.StaticToken) == "" {
			c.StaticToken = "dev-token-change-me"
		}
	}
	if c.Mode == "jwt" || c.Mode == "static_or_jwt" {
		if len(c.JWT.Audience) == 0 {
			c.JWT.Audience = []string{"trading-gateway", "trading-push"}
		}
		if len(c.JWT.Issuers) == 0 {
			return fmt.Errorf("auth: jwt mode requires at least one issuer")
		}
		for i := range c.JWT.Issuers {
			if err := c.JWT.Issuers[i].normalize(); err != nil {
				return fmt.Errorf("auth: issuer[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func (i *IssuerConfig) normalize() error {
	i.Issuer = strings.TrimSpace(i.Issuer)
	if i.Issuer == "" {
		return fmt.Errorf("issuer is required")
	}
	i.JWKSURL = strings.TrimSpace(i.JWKSURL)
	i.HS256SecretFile = strings.TrimSpace(i.HS256SecretFile)
	if i.JWKSURL == "" && i.HS256SecretFile == "" {
		return fmt.Errorf("jwks_url or hs256_secret_file is required")
	}
	if i.JWKSURL != "" && i.HS256SecretFile != "" {
		return fmt.Errorf("only one of jwks_url or hs256_secret_file")
	}
	return nil
}

// ReadSecretFile 读取 HS256 密钥文件。
func ReadSecretFile(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return nil, fmt.Errorf("secret file %q is empty", path)
	}
	return []byte(s), nil
}
