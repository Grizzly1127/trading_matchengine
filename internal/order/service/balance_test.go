package service

import (
	"testing"

	"github.com/shopspring/decimal"

	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
)

func TestToBalancePB_Available(t *testing.T) {
	row := &repository.AccountBalance{
		Asset:   "USDT",
		Balance: decimal.RequireFromString("100"),
		Frozen:  decimal.RequireFromString("30"),
	}
	pb := toBalancePB(row)
	if pb.GetAsset() != "USDT" {
		t.Fatalf("asset=%s", pb.GetAsset())
	}
	if pb.GetAvailable().GetValue() != "70" {
		t.Fatalf("available=%s", pb.GetAvailable().GetValue())
	}
}

func TestValidateBalanceUserAsset_NormalizesAsset(t *testing.T) {
	uid, asset, err := validateBalanceUserAsset(1, " usdt ")
	if err != nil {
		t.Fatal(err)
	}
	if uid != 1 || asset != "USDT" {
		t.Fatalf("uid=%d asset=%s", uid, asset)
	}
}
