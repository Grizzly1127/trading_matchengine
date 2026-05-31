package aggregator

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestAggregate_weightedMedian_filtersOutlier(t *testing.T) {
	cfg := Config{
		DeviationThreshold: decimal.NewFromFloat(0.03),
		MinSources:         2,
	}
	quotes := []Quote{
		{Source: "a", Price: decimal.NewFromInt(100), Weight: decimal.NewFromInt(1)},
		{Source: "b", Price: decimal.NewFromInt(101), Weight: decimal.NewFromInt(1)},
		{Source: "c", Price: decimal.NewFromInt(200), Weight: decimal.NewFromInt(1)}, // 异常
	}
	res, ok := Aggregate(quotes, cfg)
	if !ok {
		t.Fatal("expected ok")
	}
	if res.Price.String() != "100.5" && res.Price.String() != "100" {
		// 加权中位数在 100/101 之间
		if res.Price.LessThan(decimal.NewFromInt(100)) || res.Price.GreaterThan(decimal.NewFromInt(101)) {
			t.Fatalf("unexpected price %s", res.Price)
		}
	}
	if len(res.Sources) != 2 {
		t.Fatalf("sources=%v", res.Sources)
	}
}

func TestAggregate_insufficientSources(t *testing.T) {
	cfg := Config{
		DeviationThreshold: decimal.NewFromFloat(0.03),
		MinSources:         2,
	}
	_, ok := Aggregate([]Quote{
		{Source: "only", Price: decimal.NewFromInt(50), Weight: decimal.NewFromInt(1)},
	}, cfg)
	if ok {
		t.Fatal("expected not ok")
	}
}

func TestMedianDecimal_evenCount(t *testing.T) {
	got := medianDecimal([]decimal.Decimal{
		decimal.NewFromInt(10),
		decimal.NewFromInt(20),
	})
	if !got.Equal(decimal.NewFromInt(15)) {
		t.Fatalf("median=%s", got)
	}
}

func TestWeightedMedian_single(t *testing.T) {
	got := weightedMedian([]Quote{
		{Price: decimal.NewFromInt(42), Weight: decimal.NewFromInt(5)},
	})
	if !got.Equal(decimal.NewFromInt(42)) {
		t.Fatalf("got %s", got)
	}
}

func TestWithinDeviation_zeroMedian(t *testing.T) {
	if !withinDeviation(decimal.NewFromInt(1), decimal.Zero, decimal.NewFromFloat(0.03)) {
		t.Fatal("positive price with zero median should pass")
	}
}

func TestParseDeviation_default(t *testing.T) {
	d, err := ParseDeviation("")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Equal(decimal.NewFromFloat(0.03)) {
		t.Fatalf("got %s", d)
	}
}

func TestMedianDecimal_oddCount(t *testing.T) {
	got := medianDecimal([]decimal.Decimal{
		decimal.NewFromInt(3),
		decimal.NewFromInt(1),
		decimal.NewFromInt(2),
	})
	if !got.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("median=%s", got)
	}
}

func TestAggregate_threeEqualSources(t *testing.T) {
	quotes := []Quote{
		{Source: "a", Price: decimal.NewFromInt(100), Weight: decimal.NewFromInt(2)},
		{Source: "b", Price: decimal.NewFromInt(100), Weight: decimal.NewFromInt(1)},
		{Source: "c", Price: decimal.NewFromInt(100), Weight: decimal.NewFromInt(1)},
	}
	res, ok := Aggregate(quotes, Config{
		DeviationThreshold: decimal.NewFromFloat(0.03),
		MinSources:         2,
	})
	if !ok || !res.Price.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("ok=%v price=%s sources=%v", ok, res.Price, res.Sources)
	}
}
