package middleware

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// HeaderUserID 内网调用方指定操作用户（GET/DELETE 等无 body 时常用）。
const HeaderUserID = "X-User-Id"

// QueryUserID 查询参数名，与 JSON 字段 user_id 一致。
const QueryUserID = "user_id"

// ResolveUserID 解析用户 ID，优先级：JSON body > X-User-Id > query user_id。
func ResolveUserID(r *http.Request, fromBody uint64) (uint64, error) {
	if fromBody > 0 {
		return fromBody, nil
	}
	if id := UserIDFromContext(r.Context()); id > 0 {
		return id, nil
	}
	return parseUserIDHeaderOrQuery(r)
}

func parseUserIDHeaderOrQuery(r *http.Request) (uint64, error) {
	if h := strings.TrimSpace(r.Header.Get(HeaderUserID)); h != "" {
		return parseUserIDString(h)
	}
	if q := strings.TrimSpace(r.URL.Query().Get(QueryUserID)); q != "" {
		return parseUserIDString(q)
	}
	return 0, fmt.Errorf("user_id is required (json field, %s header, or %s query)", HeaderUserID, QueryUserID)
}

func parseUserIDString(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid user_id: %q", s)
	}
	return id, nil
}
