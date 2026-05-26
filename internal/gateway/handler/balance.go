package handler

import (
	"net/http"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/convert"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	orderv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/order/v1"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

type Balances struct {
	Balances orderv1.BalanceServiceClient
	Log      zerolog.Logger
}

type updateBalanceRequest struct {
	UserID     uint64 `json:"user_id"`
	Asset      string `json:"asset"`
	Business   string `json:"business"`
	BusinessID uint64 `json:"business_id"`
	Change     string `json:"change"`
}

func (h *Balances) UpdateBalance(w http.ResponseWriter, r *http.Request) {
	var body updateBalanceRequest
	if err := decodeJSON(w, r, &body); err != nil {
		grpcerr.Write(w, r, err)
		return
	}
	userID, ok := requireUserID(w, r, body.UserID)
	if !ok {
		return
	}

	change, err := convert.DecimalFromString(body.Change)
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest("change: "+err.Error()))
		return
	}

	req := &orderv1.UpdateBalanceRequest{
		UserId:     userID,
		Asset:      strings.ToUpper(strings.TrimSpace(body.Asset)),
		Business:   strings.TrimSpace(body.Business),
		BusinessId: body.BusinessID,
		Change:     change,
	}

	resp, err := h.Balances.UpdateBalance(r.Context(), req)
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	response.WriteOK(w, r, http.StatusOK, convert.GetBalanceResponseFromPB(resp.GetBalance()))
}

func (h *Balances) ListBalances(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r, 0)
	if !ok {
		return
	}

	h.Log.Info().Uint64("user_id", userID).Msg("List Balances request")
	resp, err := h.Balances.ListBalances(r.Context(), &orderv1.ListBalancesRequest{
		UserId: userID,
	})
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	response.WriteOK(w, r, http.StatusOK, convert.ListBalancesResponseFromPB(resp))
}

func (h *Balances) GetBalance(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r, 0)
	if !ok {
		return
	}

	asset := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "asset")))
	if asset == "" {
		grpcerr.Write(w, r, grpcerr.BadRequest("asset is required"))
		return
	}

	resp, err := h.Balances.GetBalance(r.Context(), &orderv1.GetBalanceRequest{
		UserId: userID,
		Asset:  asset,
	})
	if err != nil {
		grpcerr.Write(w, r, err)
		return
	}

	response.WriteOK(w, r, http.StatusOK, convert.GetBalanceResponseFromPB(resp.GetBalance()))
}
