package middleware

import (
	"net/http/httptest"
	"testing"
)

func TestResolveUserID_fromBody(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/orders", nil)
	id, err := ResolveUserID(r, 7)
	if err != nil || id != 7 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestResolveUserID_header(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/orders", nil)
	r.Header.Set(HeaderUserID, "42")
	id, err := ResolveUserID(r, 0)
	if err != nil || id != 42 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestResolveUserID_query(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/orders?user_id=3", nil)
	id, err := ResolveUserID(r, 0)
	if err != nil || id != 3 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestResolveUserID_bodyWins(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/orders?user_id=3", nil)
	r.Header.Set(HeaderUserID, "99")
	id, err := ResolveUserID(r, 5)
	if err != nil || id != 5 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestResolveUserID_missing(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/orders", nil)
	_, err := ResolveUserID(r, 0)
	if err == nil {
		t.Fatal("expected error")
	}
}
