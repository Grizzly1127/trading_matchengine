package auth

import "strings"

// ParseScopeClaim 解析 JWT scope（空格分隔字符串或字符串数组）。
func ParseScopeClaim(raw any) []string {
	switch v := raw.(type) {
	case string:
		return uniqueScopes(strings.Fields(v))
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return uniqueScopes(out)
	case []string:
		return uniqueScopes(v)
	default:
		return nil
	}
}

func uniqueScopes(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
