package handler

import (
	"net/http"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/grpcerr"
	gwmw "github.com/Grizzly1127/trading_matchengine/internal/gateway/middleware"
)

func requireUserID(w http.ResponseWriter, r *http.Request, fromBody uint64) (uint64, bool) {
	id, err := gwmw.ResolveUserID(r, fromBody)
	if err != nil {
		grpcerr.Write(w, r, grpcerr.BadRequest(err.Error()))
		return 0, false
	}
	return id, true
}
