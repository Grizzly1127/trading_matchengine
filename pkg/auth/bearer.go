package auth

import "strings"

// BearerFromHeader 解析 Authorization: Bearer。
func BearerFromHeader(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}
