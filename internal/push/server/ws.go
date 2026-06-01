package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/push/hub"
	"github.com/Grizzly1127/trading_matchengine/pkg/auth"
	"github.com/Grizzly1127/trading_matchengine/pkg/redis"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

type WSServer struct {
	Hub      *hub.Hub
	Redis    *redis.Client
	Verifier *auth.Verifier
	Log      zerolog.Logger
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type wsReq struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

type wsResp struct {
	Op   string      `json:"op"`
	Args []string    `json:"args,omitempty"`
	Data interface{} `json:"data,omitempty"`
	Err  string      `json:"err,omitempty"`
}

func (s *WSServer) HandleWS(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := hub.NewClient(conn)
	s.Hub.Add(c)
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
			s.handleSubscribe(r.Context(), c, req.Args)
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

func (s *WSServer) handleSubscribe(ctx context.Context, c *hub.Client, args []string) {
	accepted := make([]string, 0, len(args))
	for _, raw := range args {
		ch := strings.TrimSpace(raw)
		if !allowedChannel(ch) {
			_ = writeJSON(c, wsResp{Op: "error", Err: "unsupported channel: " + ch})
			continue
		}
		c.SetSubscribed(ch, true)
		accepted = append(accepted, ch)

		// 订阅后先发 snapshot（若 redis key 存在）。
		if s.Redis == nil {
			continue
		}
		key := snapshotKey(ch)
		if key == "" {
			continue
		}
		v, err := s.Redis.Get(ctx, key)
		if err == nil && v != "" {
			_ = c.Conn.WriteMessage(websocket.TextMessage, []byte(v))
		}
	}
	if len(accepted) > 0 {
		_ = writeJSON(c, wsResp{Op: "subscribed", Args: accepted})
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
	return strings.HasPrefix(ch, "depth:") ||
		strings.HasPrefix(ch, "ticker:") ||
		strings.HasPrefix(ch, "trade:") ||
		strings.HasPrefix(ch, "kline:") ||
		strings.HasPrefix(ch, "index:") ||
		strings.HasPrefix(ch, "ticker@all")
}

func snapshotKey(ch string) string {
	if strings.HasPrefix(ch, "depth:") || strings.HasPrefix(ch, "ticker:") || strings.HasPrefix(ch, "index:") {
		return ch
	}
	if strings.HasPrefix(ch, "ticker@all:") {
		return "ticker:all:" + strings.TrimPrefix(ch, "ticker@all:")
	}
	if ch == "ticker@all" {
		return "ticker:all:ALL"
	}
	return ""
}

func (s *WSServer) authorized(r *http.Request) bool {
	if s.Verifier == nil {
		return false
	}
	bearer, ok := auth.BearerFromHeader(r.Header.Get("Authorization"))
	if !ok && len(r.URL.Query()["token"]) > 0 {
		bearer = strings.TrimSpace(r.URL.Query().Get("token"))
		ok = bearer != ""
	}
	if !ok {
		// 兼容文档：连接后首帧 {"op":"auth","args":["<jwt>"]} 在 HandleWS 前无法用于升级；
		// 握手阶段请用 Authorization 或 ?token=。
		return false
	}
	claims, err := s.Verifier.VerifyBearer(r.Context(), bearer)
	if err != nil {
		return false
	}
	return auth.HasScopes(claims, auth.ScopePushConnect)
}
