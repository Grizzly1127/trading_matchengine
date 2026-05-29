package hub

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	Send chan []byte

	mu   sync.RWMutex
	subs map[string]struct{}
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

type Hub struct {
	mu      sync.RWMutex
	clients map[*Client]struct{}
}

func New() *Hub {
	return &Hub{clients: make(map[*Client]struct{})}
}

func (h *Hub) Add(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
}

func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
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
		select {
		case c.Send <- payload:
		default:
			// 慢客户端：丢弃本条消息，避免阻塞全局广播。
		}
	}
}
