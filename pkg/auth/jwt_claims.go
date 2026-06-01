package auth

import "github.com/golang-jwt/jwt/v5"

// ServiceClaims 服务 JWT 载荷。
type ServiceClaims struct {
	Scope string `json:"scope,omitempty"`
	jwt.RegisteredClaims
}
