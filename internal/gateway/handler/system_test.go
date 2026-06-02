package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	Health(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var env struct {
		Code int `json:"code"`
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Code != 0 || env.Data.Status != "ok" {
		t.Fatalf("body=%s", rec.Body.String())
	}
}

func TestTime(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/time", nil)
	rec := httptest.NewRecorder()
	Time(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var env struct {
		Data struct {
			ServerTime string `json:"server_time"`
			UnixMs     int64  `json:"unix_ms"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Data.ServerTime == "" || env.Data.UnixMs <= 0 {
		t.Fatalf("body=%s", rec.Body.String())
	}
}
