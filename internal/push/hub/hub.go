package hub

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Grizzly1127/trading_matchengine/internal/push/limits"
	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	Send chan []byte

	mu          sync.RWMutex
	subs        map[string]struct{}
	UserID      uint64 // 订阅 order 频道时绑定，用于 order:{user_id} 路由
	Subject     string
	MarketMaker bool
}

func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		Conn: conn,
		Send: make(chan []byte, 256),
		subs: make(map[string]struct{}),
	}
}

func (c *Client) SetSubscribed(ch string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if on {
		c.subs[ch] = struct{}{}
		return
	}
	delete(c.subs, ch)
}

func (c *Client) IsSubscribed(ch string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.subs[ch]
	return ok
}

// SubscribedChannels 返回当前订阅列表副本。
func (c *Client) SubscribedChannels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.subs))
	for ch := range c.subs {
		out = append(out, ch)
	}
	return out
}

type Hub struct {
	mu      sync.RWMutex
	clients map[*Client]struct{}
	limits  limits.Config
}

func New() *Hub {
	return &Hub{clients: make(map[*Client]struct{}), limits: limits.Config{}.WithDefaults()}
}

// NewWithLimits 创建带限流配置的 Hub。
func NewWithLimits(cfg limits.Config) *Hub {
	return &Hub{clients: make(map[*Client]struct{}), limits: cfg.WithDefaults()}
}

// CanRegister 握手前检查 subject 是否还可建立新连接。
func (h *Hub) CanRegister(subject string, marketMaker bool) bool {
	if subject == "" {
		subject = "anonymous"
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	limit := h.limits.MaxConnections(marketMaker)
	n := 0
	for client := range h.clients {
		if client.Subject == subject {
			n++
		}
	}
	return n < limit
}

// Register 登记连接并校验 subject 并发连接数。
func (h *Hub) Register(c *Client, subject string, marketMaker bool) error {
	if c == nil {
		return fmt.Errorf("hub: client is nil")
	}
	if subject == "" {
		subject = "anonymous"
	}
	c.Subject = subject
	c.MarketMaker = marketMaker

	h.mu.Lock()
	defer h.mu.Unlock()

	limit := h.limits.MaxConnections(marketMaker)
	n := 0
	for client := range h.clients {
		if client.Subject == subject {
			n++
		}
	}
	if n >= limit {
		return ErrTooManyConnections
	}
	h.clients[c] = struct{}{}
	return nil
}

func (h *Hub) Add(c *Client) {
	_ = h.Register(c, "", false)
}

func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	close(c.Send)
}

func (h *Hub) Broadcast(channel string, payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.IsSubscribed(channel) {
			continue
		}
		h.send(c, payload)
	}
}

// BroadcastOrder 将 order:{user_id} 消息扇出到订阅 order / order:{user_id} 的客户端。
func (h *Hub) BroadcastOrder(userID uint64, payload []byte) {
	if userID == 0 {
		return
	}
	specific := fmt.Sprintf("order:%d", userID)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if c.IsSubscribed(specific) || (c.IsSubscribed("order") && c.UserID == userID) {
			h.send(c, payload)
		}
	}
}

func (h *Hub) send(c *Client, payload []byte) {
	select {
	case c.Send <- payload:
	default:
		// 慢客户端：丢弃本条消息，避免阻塞全局广播。
	}
}

// ParseOrderChannel 从 Redis 频道 order:{user_id} 解析用户 ID。
func ParseOrderChannel(channel string) (uint64, bool) {
	if !strings.HasPrefix(channel, "order:") {
		return 0, false
	}
	uid, err := strconv.ParseUint(strings.TrimPrefix(channel, "order:"), 10, 64)
	if err != nil || uid == 0 {
		return 0, false
	}
	return uid, true
}
