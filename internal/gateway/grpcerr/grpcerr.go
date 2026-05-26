package grpcerr

import (
	"errors"
	"net/http"
	"strings"

	"github.com/Grizzly1127/trading_matchengine/internal/gateway/response"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// APIError 为 REST 层错误码与 HTTP 状态。
type APIError struct {
	HTTPStatus int
	Code       int
	Message    string
}

func (e *APIError) Error() string {
	return e.Message
}

// Map 将 gRPC status 映射为 REST 错误（rest-api §2.4）。
func Map(err error) *APIError {
	if err == nil {
		return nil
	}
	var api *APIError
	if errors.As(err, &api) {
		return api
	}

	st, ok := status.FromError(err)
	if !ok {
		return &APIError{
			HTTPStatus: http.StatusInternalServerError,
			Code:       50000,
			Message:    "internal server error",
		}
	}

	msg := st.Message()
	switch st.Code() {
	case codes.InvalidArgument:
		return &APIError{HTTPStatus: http.StatusBadRequest, Code: 40000, Message: msg}
	case codes.Unauthenticated:
		return &APIError{HTTPStatus: http.StatusUnauthorized, Code: 40100, Message: msg}
	case codes.NotFound:
		return &APIError{HTTPStatus: http.StatusNotFound, Code: 40400, Message: msg}
	case codes.FailedPrecondition:
		return mapFailedPrecondition(msg)
	case codes.Unavailable:
		return &APIError{HTTPStatus: http.StatusServiceUnavailable, Code: 50300, Message: msg}
	default:
		return &APIError{HTTPStatus: http.StatusInternalServerError, Code: 50000, Message: msg}
	}
}

func mapFailedPrecondition(msg string) *APIError {
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "insufficient") || strings.Contains(lower, "balance") {
		return &APIError{HTTPStatus: http.StatusUnprocessableEntity, Code: 42201, Message: msg}
	}
	if strings.Contains(lower, "cancel") {
		return &APIError{HTTPStatus: http.StatusUnprocessableEntity, Code: 42202, Message: msg}
	}
	return &APIError{HTTPStatus: http.StatusUnprocessableEntity, Code: 42201, Message: msg}
}

// Write 将错误写入统一 JSON 响应。
func Write(w http.ResponseWriter, r *http.Request, err error) {
	api := Map(err)
	if api == nil {
		return
	}
	response.WriteError(w, r, api.HTTPStatus, api.Code, api.Message)
}

// BadRequest 返回参数类错误（未调 gRPC 前的 Gateway 校验）。
func BadRequest(message string) *APIError {
	return &APIError{HTTPStatus: http.StatusBadRequest, Code: 40000, Message: message}
}
