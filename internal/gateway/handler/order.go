package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/convert"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	gwmw "github.com/Grizzly1127/trading_matchengine/internal/gateway/middleware"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxOrderBodyBytes = 1 << 20

// Orders 订单 REST handler。
type Orders struct {
	Orders orderv1.OrderServiceClient
	Log    zerolog.Logger
}

type placeOrderRequest struct {
	UserID        uint64 `json:"user_id"`
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	TimeInForce   string `json:"time_in_force"`
}

// PlaceOrder POST /v1/orders
func (h *Orders) PlaceOrder(w http.ResponseWriter, r *http.Request) {
	var body placeOrderRequest
	if err := decodeJSON(w, r, &body); err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
		return
	}
	userID, ok := requireUserID(w, r, body.UserID)
	if !ok {
		return
	}
	_ = body.TimeInForce // Phase 1 忽略

	side, err := convert.ParseSide(body.Side)
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
		return
	}
	typ, err := convert.ParseOrderType(body.Type)
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
		return
	}
	qty, err := convert.DecimalFromString(body.Quantity)
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest("quantity: "+err.Error()))
		return
	}

	req := &orderv1.PlaceOrderRequest{
		UserId:        userID,
		ClientOrderId: strings.TrimSpace(body.ClientOrderID),
		Symbol:        strings.TrimSpace(body.Symbol),
		Side:          side,
		Type:          typ,
		Quantity:      qty,
	}
	if typ == commonv1.OrderType_ORDER_TYPE_LIMIT {
		price, err := convert.DecimalFromString(body.Price)
		if err != nil {
			grpcerr.Write(w, r, grpcerr.BadRequest("price: "+err.Error()))
			return
		}
		req.Price = price
	}

	resp, err := h.Orders.PlaceOrder(r.Context(), req)
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	status := http.StatusCreated
	if resp.GetIdempotentHit() {
		status = http.StatusOK
	}
	response.WriteOK(w, r, status, convert.PlaceOrderResponseFromPB(resp))
}

// CancelOrder DELETE /v1/orders/{order_id}
func (h *Orders) CancelOrder(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r, 0)
	if !ok {
		return
	}

	orderID, err := convert.ParseOrderID(chi.URLParam(r, "order_id"))
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
		return
	}

	resp, err := h.Orders.CancelOrder(r.Context(), &orderv1.CancelOrderRequest{
		UserId:  userID,
		OrderId: orderID,
	})
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	if sym := strings.TrimSpace(r.URL.Query().Get("symbol")); sym != "" && sym != resp.GetSymbol() {
		h.Log.Warn().
			Str("request_id", gwmw.RequestIDFromContext(r.Context())).
			Str("query_symbol", sym).
			Str("order_symbol", resp.GetSymbol()).
			Uint64("order_id", orderID).
			Msg("cancel symbol mismatch")
	}

	response.WriteOK(w, r, http.StatusOK, convert.CancelOrderResponseFromPB(resp))
}

// GetOrder GET /v1/orders/{order_id}
func (h *Orders) GetOrder(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r, 0)
	if !ok {
		return
	}

	orderID, err := convert.ParseOrderID(chi.URLParam(r, "order_id"))
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
		return
	}

	resp, err := h.Orders.GetOrder(r.Context(), &orderv1.GetOrderRequest{
		UserId:  userID,
		OrderId: orderID,
	})
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	response.WriteOK(w, r, http.StatusOK, convert.OrderFromPB(resp.GetOrder()))
}

// ListOrders GET /v1/orders
func (h *Orders) ListOrders(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r, 0)
	if !ok {
		return
	}

	q := r.URL.Query()
	statuses := convert.ParseStatusList(q.Get("status"))

	pageSize := 50
	if limitStr := strings.TrimSpace(q.Get("limit")); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit < 1 {
			grpcerr.Write(w, r, grpcerr.BadRequest("limit must be a positive integer"))
			return
		}
		if limit > 200 {
			limit = 200
		}
		pageSize = limit
	}
	if pageSize > 100 {
		pageSize = 100
	}

	req := &orderv1.ListOrdersRequest{
		UserId:   userID,
		Symbol:   strings.TrimSpace(q.Get("symbol")),
		Page:     1,
		PageSize: int32(pageSize),
	}

	if sideStr := strings.TrimSpace(q.Get("side")); sideStr != "" {
		side, err := convert.ParseSide(sideStr)
		if err != nil {
			grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
			return
		}
		req.Side = side
	}

	if start := strings.TrimSpace(q.Get("start_time")); start != "" {
		t, err := convert.ParseTimeQuery(start)
		if err != nil {
			grpcerr.Write(w, r, grpcerr.BadRequest("start_time: "+err.Error()))
			return
		}
		req.CreatedAtFrom = timestamppb.New(t)
	}
	if end := strings.TrimSpace(q.Get("end_time")); end != "" {
		t, err := convert.ParseTimeQuery(end)
		if err != nil {
			grpcerr.Write(w, r, grpcerr.BadRequest("end_time: "+err.Error()))
			return
		}
		req.CreatedAtTo = timestamppb.New(t)
	}

	if len(statuses) == 1 {
		req.Status = statuses[0]
	}

	resp, err := h.Orders.ListOrders(r.Context(), req)
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	items := convert.OrdersFromPB(resp.GetOrders())
	if len(statuses) > 1 {
		items = convert.FilterOrdersByStatus(items, statuses)
	}

	hasMore := len(items) >= pageSize
	data := convert.OrderListData{
		Items:   items,
		HasMore: hasMore,
	}
	response.WriteOK(w, r, http.StatusOK, data)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxOrderBodyBytes)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if err == io.EOF {
			return grpcerr.BadRequest("request body is required")
		}
		return grpcerr.BadRequest(err.Error())
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return grpcerr.BadRequest("request body must be a single JSON object")
	}
	return nil
}
