package convert

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// FormatTimeUTC 格式化为 ISO 8601 UTC（毫秒）。
func FormatTimeUTC(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// FormatTimestamp 格式化 proto Timestamp；nil 返回空串。
func FormatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() {
		return ""
	}
	return FormatTimeUTC(ts.AsTime())
}

// ParseTimeQuery 解析 REST 查询中的 ISO 8601 时间。
func ParseTimeQuery(s string) (time.Time, error) {
	s = trim(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("time is empty")
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
	}
	var lastErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("invalid time %q: %v", s, lastErr)
}

// ParseStatusList 解析逗号分隔的 status 列表（大写、去空）。
func ParseStatusList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.ToUpper(p))
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}
