package repository

import "testing"

func TestComputeFreeze_LimitBuy(t *testing.T) {
	price := "100"
	spec, err := ComputeFreeze(1, "BTC-USDT", &price, "0.5", nil)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "USDT" || spec.Amount.String() != "50" {
		t.Fatalf("spec=%+v", spec)
	}
}

func TestComputeFreeze_LimitSell(t *testing.T) {
	spec, err := ComputeFreeze(2, "BTC-USDT", nil, "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "BTC" || spec.Amount.String() != "1" {
		t.Fatalf("spec=%+v", spec)
	}
}

func TestComputeFreeze_BuyWithFrozenAmount(t *testing.T) {
	amt := "123.45"
	spec, err := ComputeFreeze(1, "BTC-USDT", nil, "1", &amt)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "USDT" || spec.Amount.String() != "123.45" {
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

func TestRemainingFreeze_BuyWithFrozenAmount(t *testing.T) {
	o := &Order{
		Symbol:         "BTC-USDT",
		Side:           1,
		Quantity:       "10",
		FilledQuantity: "2.5",
		FrozenAmount:   strPtr("1000"),
	}
	spec, err := RemainingFreeze(o)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Asset != "USDT" || spec.Amount.String() != "750" {
		t.Fatalf("spec=%+v", spec)
	}
}

func strPtr(s string) *string { return &s }
