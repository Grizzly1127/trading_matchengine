package repository

import "testing"

func TestComputeFreeze_LimitBuy(t *testing.T) {
	price := "100"
	spec, err := ComputeFreeze(1, "BTC-USDT", &price, "0.5")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "USDT" || spec.Amount.String() != "50" {
		t.Fatalf("spec=%+v", spec)
	}
}

func TestComputeFreeze_LimitSell(t *testing.T) {
	spec, err := ComputeFreeze(2, "BTC-USDT", nil, "1")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "BTC" || spec.Amount.String() != "1" {
		t.Fatalf("spec=%+v", spec)
	}
}

func TestRemainingFreeze_PartialSell(t *testing.T) {
	o := &Order{
		Symbol:         "BTC-USDT",
		Side:           2,
		Quantity:       "1",
		FilledQuantity: "0.3",
	}
	spec, err := RemainingFreeze(o)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "BTC" || spec.Amount.String() != "0.7" {
		t.Fatalf("spec=%+v", spec)
	}
}
