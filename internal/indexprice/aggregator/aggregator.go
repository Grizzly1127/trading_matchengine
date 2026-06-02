package aggregator

import (
	"fmt"
	"sort"

	"github.com/shopspring/decimal"
)

// Quote 单源报价。
type Quote struct {
	Source string
	Price  decimal.Decimal
	Weight decimal.Decimal
}

// Result 聚合结果。
type Result struct {
	Price   decimal.Decimal
	Sources []string
}

// Config 聚合参数。
type Config struct {
	// DeviationThreshold 相对中位数的最大偏差比例（如 0.03 = 3%）。
	DeviationThreshold decimal.Decimal
	MinSources         int
}

// Aggregate 先算中位数过滤异常值，再对剩余源做加权中位数。
func Aggregate(quotes []Quote, cfg Config) (Result, bool) {
	if cfg.MinSources <= 0 {
		cfg.MinSources = 1
	}
	if len(quotes) == 0 {
		return Result{}, false
	}

	prices := make([]decimal.Decimal, 0, len(quotes))
	for _, q := range quotes {
		if q.Price.IsPositive() {
			prices = append(prices, q.Price)
		}
	}
	if len(prices) == 0 {
		return Result{}, false
	}

	median := medianDecimal(prices)
	filtered := make([]Quote, 0, len(quotes))
	for _, q := range quotes {
		if !q.Price.IsPositive() {
			continue
		}
		if !withinDeviation(q.Price, median, cfg.DeviationThreshold) {
			continue
		}
		w := q.Weight
		if !w.IsPositive() {
			w = decimal.NewFromInt(1)
		}
		filtered = append(filtered, Quote{
			Source: q.Source,
			Price:  q.Price,
			Weight: w,
		})
	}
	if len(filtered) < cfg.MinSources {
		return Result{}, false
	}

	price := weightedMedian(filtered)
	sources := make([]string, 0, len(filtered))
	for _, q := range filtered {
		sources = append(sources, q.Source)
	}
	sort.Strings(sources)
	return Result{Price: price, Sources: sources}, true
}

func withinDeviation(price, median, threshold decimal.Decimal) bool {
	if !median.IsPositive() {
		return price.IsPositive()
	}
	if threshold.IsNegative() {
		return false
	}
	diff := price.Sub(median).Abs()
	ratio := diff.Div(median)
	return !ratio.GreaterThan(threshold)
}

func medianDecimal(values []decimal.Decimal) decimal.Decimal {
	if len(values) == 0 {
		return decimal.Zero
	}
	sorted := append([]decimal.Decimal(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LessThan(sorted[j])
	})
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return sorted[mid-1].Add(sorted[mid]).Div(decimal.NewFromInt(2))
}

func weightedMedian(quotes []Quote) decimal.Decimal {
	if len(quotes) == 0 {
		return decimal.Zero
	}
	if len(quotes) == 1 {
		return quotes[0].Price
	}

	sorted := append([]Quote(nil), quotes...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Price.LessThan(sorted[j].Price)
	})

	total := decimal.Zero
	for _, q := range sorted {
		total = total.Add(q.Weight)
	}
	if !total.IsPositive() {
		return sorted[len(sorted)/2].Price
	}
	half := total.Div(decimal.NewFromInt(2))
	cum := decimal.Zero
	for _, q := range sorted {
		cum = cum.Add(q.Weight)
		if cum.GreaterThanOrEqual(half) {
			return q.Price
		}
	}
	return sorted[len(sorted)-1].Price
}

// ParseDeviation 解析偏差阈值字符串。
func ParseDeviation(s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.NewFromFloat(0.03), nil
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("aggregator: invalid deviation threshold %q: %w", s, err)
	}
	if d.IsNegative() {
		return decimal.Decimal{}, fmt.Errorf("aggregator: deviation threshold must be non-negative")
	}
	return d, nil
}
