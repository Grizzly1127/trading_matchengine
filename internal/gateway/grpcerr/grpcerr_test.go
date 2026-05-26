package grpcerr

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMap_InvalidArgument(t *testing.T) {
	err := status.Error(codes.InvalidArgument, "bad field")
	api := Map(err)
	if api.HTTPStatus != http.StatusBadRequest || api.Code != 40000 {
		t.Fatalf("api=%+v", api)
	}
}

func TestMap_InsufficientBalance(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, "insufficient balance")
	api := Map(err)
	if api.Code != 42201 {
		t.Fatalf("code=%d", api.Code)
	}
}

func TestMap_NotCancelable(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, "order not cancelable")
	api := Map(err)
	if api.Code != 42202 {
		t.Fatalf("code=%d", api.Code)
	}
}
