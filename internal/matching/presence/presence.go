package presence

// Kind 撮合侧订单存在性（对 Order Service 补偿可见）。
type Kind int

const (
	Unknown Kind = iota
	InOrderbook
	KnownNotInOrderbook
)
