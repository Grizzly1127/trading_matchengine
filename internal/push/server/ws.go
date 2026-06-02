package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/internal/push/limits"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/Grizzly1127/trading_matchengine/pkg/tickerall"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

type WSServer struct {
	Hub      *hub.Hub
	Redis    *redis.Client
	Verifier *auth.Verifier
	Limits   limits.Config
	Log      zerolog.Logger
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type wsReq struct {
	Op     string   `json:"op"`
	Args   []string `json:"args"`
	UserID uint64   `json:"user_id,omitempty"`
}

type wsResp struct {
	Op   string      `json:"op"`
	Args []string    `json:"args,omitempty"`
	Data interface{} `json:"data,omitempty"`
	Err  string      `json:"err,omitempty"`
}

func (s *WSServer) HandleWS(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.verifyClaims(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	marketMaker := auth.HasScopes(claims, auth.ScopePushTickerAll)
	if !s.Hub.CanRegister(claims.Subject, marketMaker) {
		http.Error(w, hub.ErrTooManyConnections.Error(), http.StatusTooManyRequests)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := hub.NewClient(conn)
	if err := s.Hub.Register(c, claims.Subject, marketMaker); err != nil {
		_ = conn.Close()
		return
	}
	defer func() {
		s.Hub.Remove(c)
		_ = conn.Close()
	}()

	go writePump(c)
	_ = writeJSON(c, wsResp{Op: "connected"})

	for {
		var req wsReq
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		switch strings.ToLower(strings.TrimSpace(req.Op)) {
		case "subscribe":
			s.handleSubscribe(r.Context(), c, req.Args, req.UserID)
		case "unsubscribe":
			for _, ch := range req.Args {
				c.SetSubscribed(strings.TrimSpace(ch), false)
			}
			_ = writeJSON(c, wsResp{Op: "unsubscribed", Args: req.Args})
		case "ping":
			_ = writeJSON(c, wsResp{Op: "pong"})
		default:
			_ = writeJSON(c, wsResp{Op: "error", Err: "unknown op"})
		}
	}
}

func (s *WSServer) handleSubscribe(ctx context.Context, c *hub.Client, args []string, reqUserID uint64) {
	cfg := s.Limits.WithDefaults()
	accepted := make([]string, 0, len(args))
	pending := make([]string, 0, len(args))
	for _, raw := range args {
		ch := strings.TrimSpace(raw)
		if !allowedChannel(ch) {
			_ = writeJSON(c, wsResp{Op: "error", Err: "unsupported channel: " + ch})
			continue
		}
		if !c.MarketMaker && limits.IsTickerAllChannel(ch) {
			_ = writeJSON(c, wsResp{Op: "error", Err: "ticker@all requires market maker scope"})
			continue
		}
		pending = append(pending, ch)
	}
	if !c.MarketMaker && len(pending) > 0 {
		total := limits.MergeSymbolCount(c.SubscribedChannels(), pending)
		if total > cfg.RetailMaxSymbolsPerConnection {
			_ = writeJSON(c, wsResp{Op: "error", Err: fmt.Sprintf(
				"symbol subscription limit exceeded (max %d)", cfg.RetailMaxSymbolsPerConnection)})
			return
		}
	}
	var snapshots [][]byte
	for _, ch := range pending {
		if ch == "order" {
			if reqUserID == 0 {
				_ = writeJSON(c, wsResp{Op: "error", Err: "subscribe order requires user_id in frame"})
				continue
			}
			c.UserID = reqUserID
		}
		if uid, ok := parseOrderChannelArg(ch); ok {
			c.UserID = uid
		}
		c.SetSubscribed(ch, true)
		accepted = append(accepted, ch)

		if s.Redis == nil {
			continue
		}
		key := snapshotKey(ch)
		if key == "" {
			continue
		}
		v, err := s.Redis.Get(ctx, key)
		if err != nil || v == "" {
			continue
		}
		payload := []byte(v)
		if limits.IsTickerAllChannel(ch) {
			frame, err := tickerall.WSSnapshotFromRedisREST(ch, payload)
			if err != nil {
				continue
			}
			payload = frame
		}
		snapshots = append(snapshots, payload)
	}
	if len(accepted) > 0 {
		_ = writeJSON(c, wsResp{Op: "subscribed", Args: accepted})
	}
	// §8.2：subscribed 确认后再推送 snapshot。
	for _, payload := range snapshots {
		_ = c.Conn.WriteMessage(websocket.TextMessage, payload)
	}
}

func writePump(c *hub.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-c.Send:
			if !ok {
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.Conn.WriteMessage(websocket.TextMessage, []byte(`{"op":"heartbeat"}`)); err != nil {
				return
			}
		}
	}
}

func writeJSON(c *hub.Client, resp wsResp) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	return c.Conn.WriteMessage(websocket.TextMessage, b)
}

func allowedChannel(ch string) bool {
	if ch == "order" {
		return true
	}
	if _, ok := parseOrderChannelArg(ch); ok {
		return true
	}
	return strings.HasPrefix(ch, "depth:") ||
		strings.HasPrefix(ch, "ticker:") ||
		strings.HasPrefix(ch, "trade:") ||
		strings.HasPrefix(ch, "kline:") ||
		strings.HasPrefix(ch, "index:") ||
		strings.HasPrefix(ch, "ticker@all")
}

func parseOrderChannelArg(ch string) (uint64, bool) {
	if !strings.HasPrefix(ch, "order:") {
		return 0, false
	}
	uid, err := strconv.ParseUint(strings.TrimPrefix(ch, "order:"), 10, 64)
	if err != nil || uid == 0 {
		return 0, false
	}
	return uid, true
}

func snapshotKey(ch string) string {
	if strings.HasPrefix(ch, "depth:") || strings.HasPrefix(ch, "ticker:") || strings.HasPrefix(ch, "index:") {
		return ch
	}
	if strings.HasPrefix(ch, "ticker@all:") {
		return tickerall.RedisKey(strings.TrimPrefix(ch, "ticker@all:"))
	}
	if ch == "ticker@all" {
		return tickerall.RedisKey("ALL")
	}
	return ""
}

func (s *WSServer) verifyClaims(r *http.Request) (auth.Claims, bool) {
	if s.Verifier == nil {
		return auth.Claims{}, false
	}
	bearer, ok := auth.BearerFromHeader(r.Header.Get("Authorization"))
	if !ok && len(r.URL.Query()["token"]) > 0 {
		bearer = strings.TrimSpace(r.URL.Query().Get("token"))
		ok = bearer != ""
	}
	if !ok {
		return auth.Claims{}, false
	}
	claims, err := s.Verifier.VerifyBearer(r.Context(), bearer)
	if err != nil {
		return auth.Claims{}, false
	}
	if !auth.HasScopes(claims, auth.ScopePushConnect) {
		return auth.Claims{}, false
	}
	return claims, true
}
