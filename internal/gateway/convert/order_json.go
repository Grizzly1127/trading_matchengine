package convert

import (
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
)

// PlaceOrderData POST /v1/orders 响应 data。
type PlaceOrderData struct {
	OrderID       string `json:"order_id"`
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
}

// CancelOrderData DELETE /v1/orders/{id} 响应 data。
type CancelOrderData struct {
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updated_at"`
}

// OrderJSON GET 订单对象（rest-api §3.4）。
type OrderJSON struct {
	OrderID        string `json:"order_id"`
	ClientOrderID  string `json:"client_order_id"`
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	Type           string `json:"type"`
	Price          string `json:"price"`
	Quantity       string `json:"quantity"`
	FilledQuantity string `json:"filled_quantity"`
	AvgPrice       string `json:"avg_price"`
	Status         string `json:"status"`
	TimeInForce    string `json:"time_in_force"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

// OrderListData GET /v1/orders 列表 data。
type OrderListData struct {
	Items   []OrderJSON `json:"items"`
	Cursor  string      `json:"cursor,omitempty"`
	HasMore bool        `json:"has_more"`
}

// PlaceOrderResponseFromPB 映射下单响应。
func PlaceOrderResponseFromPB(resp *orderv1.PlaceOrderResponse) PlaceOrderData {
	if resp == nil {
		return PlaceOrderData{}
	}
	return PlaceOrderData{
		OrderID:       OrderIDToString(resp.GetOrderId()),
		ClientOrderID: resp.GetClientOrderId(),
		Symbol:        resp.GetSymbol(),
		Status:        resp.GetStatus(),
		CreatedAt:     FormatTimestamp(resp.GetCreatedAt()),
	}
}

// CancelOrderResponseFromPB 映射撤单响应。
func CancelOrderResponseFromPB(resp *orderv1.CancelOrderResponse) CancelOrderData {
	if resp == nil {
		return CancelOrderData{}
	}
	return CancelOrderData{
		OrderID:   OrderIDToString(resp.GetOrderId()),
		Status:    resp.GetStatus(),
		UpdatedAt: "",
	}
}

// OrderFromPB 映射 OrderInfo。
func OrderFromPB(o *orderv1.OrderInfo) OrderJSON {
	if o == nil {
		return OrderJSON{}
	}
	return OrderJSON{
		OrderID:        OrderIDToString(o.GetOrderId()),
		ClientOrderID:  o.GetClientOrderId(),
		Symbol:         o.GetSymbol(),
		Side:           SideToString(o.GetSide()),
		Type:           OrderTypeToString(o.GetType()),
		Price:          DecimalToString(o.GetPrice()),
		Quantity:       DecimalToString(o.GetQuantity()),
		FilledQuantity: DecimalToString(o.GetFilledQuantity()),
		AvgPrice:       "",
		Status:         o.GetStatus(),
		TimeInForce:    "",
		CreatedAt:      FormatTimestamp(o.GetCreatedAt()),
		UpdatedAt:      FormatTimestamp(o.GetUpdatedAt()),
	}
}

// OrdersFromPB 批量映射。
func OrdersFromPB(orders []*orderv1.OrderInfo) []OrderJSON {
	out := make([]OrderJSON, 0, len(orders))
	for _, o := range orders {
		out = append(out, OrderFromPB(o))
	}
	return out
}

// FilterOrdersByStatus 在 Gateway 侧按多 status 过滤（gRPC 仅支持单 status）。
func FilterOrdersByStatus(items []OrderJSON, statuses []string) []OrderJSON {
	if len(statuses) == 0 {
		return items
	}
	allow := make(map[string]struct{}, len(statuses))
	for _, s := range statuses {
		allow[s] = struct{}{}
	}
	out := make([]OrderJSON, 0, len(items))
	for _, item := range items {
		if _, ok := allow[item.Status]; ok {
			out = append(out, item)
		}
	}
	return out
}
