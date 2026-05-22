package cli

// Command 是单行 JSONL 命令。
type Command struct {
	Op            string `json:"op"`
	OrderID       uint64 `json:"order_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	ClientOrderID string `json:"client_order_id"`
	CommandID     uint64 `json:"command_id"`
}

// Response 是 stdout 输出的 JSON 结果。
type Response struct {
	OK           bool    `json:"ok"`
	Op           string  `json:"op,omitempty"`
	Error        string  `json:"error,omitempty"`
	Duplicate    bool    `json:"duplicate,omitempty"`
	Trades       []Trade `json:"trades,omitempty"`
	LastSeq      uint64  `json:"last_seq,omitempty"`
	ActiveCount  int     `json:"active_count,omitempty"`
	BestBid      string  `json:"best_bid,omitempty"`
	BestAsk      string  `json:"best_ask,omitempty"`
	ActiveOrders []Order `json:"active_orders,omitempty"`
}

// Trade 是成交摘要（JSON 友好）。
type Trade struct {
	TradeID      uint64 `json:"trade_id"`
	Symbol       string `json:"symbol"`
	Price        string `json:"price"`
	Quantity     string `json:"quantity"`
	MakerOrderID uint64 `json:"maker_order_id"`
	TakerOrderID uint64 `json:"taker_order_id"`
}

// Order 是挂单摘要（JSON 友好）。
type Order struct {
	OrderID   uint64 `json:"order_id"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Remaining string `json:"remaining"`
}
