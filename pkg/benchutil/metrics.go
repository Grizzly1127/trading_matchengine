package benchutil

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// CounterSample 解析自 Prometheus text exposition 的 counter。
type CounterSample struct {
	Name  string
	Value float64
}

// HistogramSnapshot 直方图快照，用于计算分位数。
type HistogramSnapshot struct {
	Name    string
	Buckets []BucketCount // 按 Le 升序
	Count   float64
	Sum   float64
}

// BucketCount 单个 le 桶。
type BucketCount struct {
	Le    float64
	Count float64
}

// ParseCounters 读取 metrics 文本中的 counter（仅 _total 或无后缀的 counter 行）。
func ParseCounters(r io.Reader, names ...string) (map[string]float64, error) {
	want := make(map[string]struct{}, len(names))
	for _, n := range names {
		want[n] = struct{}{}
	}
	out := make(map[string]float64, len(names))
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		name, val, ok := parseSampleLine(line)
		if !ok {
			continue
		}
		if _, ok := want[name]; ok {
			out[name] = val
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for _, n := range names {
		if _, ok := out[n]; !ok {
			out[n] = 0
		}
	}
	return out, nil
}

// ParseHistogram 解析指定 histogram 的 _bucket/_count/_sum 行。
func ParseHistogram(r io.Reader, name string) (HistogramSnapshot, error) {
	var snap HistogramSnapshot
	snap.Name = name
	bucketPrefix := name + "_bucket"
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		if strings.HasPrefix(line, bucketPrefix) {
			le, val, err := parseHistogramBucket(line, bucketPrefix)
			if err != nil {
				return snap, err
			}
			snap.Buckets = append(snap.Buckets, BucketCount{Le: le, Count: val})
			continue
		}
		if strings.HasPrefix(line, name+"_count") {
			_, snap.Count, _ = parseSampleLine(line)
			continue
		}
		if strings.HasPrefix(line, name+"_sum") {
			_, snap.Sum, _ = parseSampleLine(line)
		}
	}
	if err := sc.Err(); err != nil {
		return snap, err
	}
	sort.Slice(snap.Buckets, func(i, j int) bool {
		return snap.Buckets[i].Le < snap.Buckets[j].Le
	})
	return snap, nil
}

// SubtractHistogram 从累积直方图 post 减去 pre，得到时间窗口内新增样本的分布。
// 用于 L2 压测 load 段（metrics-post − metrics-pre）。
func SubtractHistogram(post, pre HistogramSnapshot) (HistogramSnapshot, error) {
	if post.Name != pre.Name {
		return HistogramSnapshot{}, fmt.Errorf("benchutil: histogram name mismatch %q vs %q", post.Name, pre.Name)
	}
	out := HistogramSnapshot{Name: post.Name}
	out.Count = post.Count - pre.Count
	out.Sum = post.Sum - pre.Sum
	if out.Count < 0 || out.Sum < 0 {
		return HistogramSnapshot{}, fmt.Errorf("benchutil: negative delta for %s (count=%.0f sum=%.0f)", post.Name, out.Count, out.Sum)
	}
	preByLe := make(map[float64]float64, len(pre.Buckets))
	for _, b := range pre.Buckets {
		preByLe[b.Le] = b.Count
	}
	seen := make(map[float64]struct{}, len(post.Buckets))
	for _, b := range post.Buckets {
		delta := b.Count - preByLe[b.Le]
		if delta < 0 {
			return HistogramSnapshot{}, fmt.Errorf("benchutil: negative bucket delta le=%v for %s", b.Le, post.Name)
		}
		out.Buckets = append(out.Buckets, BucketCount{Le: b.Le, Count: delta})
		seen[b.Le] = struct{}{}
	}
	for _, b := range pre.Buckets {
		if _, ok := seen[b.Le]; ok {
			continue
		}
		out.Buckets = append(out.Buckets, BucketCount{Le: b.Le, Count: 0})
	}
	sort.Slice(out.Buckets, func(i, j int) bool {
		return out.Buckets[i].Le < out.Buckets[j].Le
	})
	return out, nil
}

// Quantile 由累积直方图桶估算分位数（毫秒指标直接读 Le）。
func (h HistogramSnapshot) Quantile(q float64) float64 {
	if q <= 0 || len(h.Buckets) == 0 || h.Count <= 0 {
		return 0
	}
	if q >= 1 {
		return h.Buckets[len(h.Buckets)-1].Le
	}
	target := h.Count * q
	var prevLe float64
	var prevCount float64
	for _, b := range h.Buckets {
		if b.Count >= target {
			if b.Count == prevCount {
				return b.Le
			}
			// 线性插值
			rank := (target - prevCount) / (b.Count - prevCount)
			if math.IsInf(b.Le, 1) {
				return prevLe
			}
			return prevLe + rank*(b.Le-prevLe)
		}
		prevLe = b.Le
		prevCount = b.Count
	}
	return h.Buckets[len(h.Buckets)-1].Le
}

func parseSampleLine(line string) (name string, value float64, ok bool) {
	sp := strings.Fields(line)
	if len(sp) < 2 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(sp[len(sp)-1], 64)
	if err != nil {
		return "", 0, false
	}
	name = sp[0]
	if idx := strings.IndexByte(name, '{'); idx >= 0 {
		name = name[:idx]
	}
	return name, v, true
}

func parseHistogramBucket(line, prefix string) (le float64, count float64, err error) {
	// matching_processing_latency_ms_bucket{le="10"} 123
	idx := strings.Index(line, "le=\"")
	if idx < 0 {
		return 0, 0, fmt.Errorf("benchutil: missing le in %q", line)
	}
	end := strings.Index(line[idx+4:], "\"")
	if end < 0 {
		return 0, 0, fmt.Errorf("benchutil: bad le in %q", line)
	}
	leStr := line[idx+4 : idx+4+end]
	if leStr == "+Inf" {
		le = math.Inf(1)
	} else {
		le, err = strconv.ParseFloat(leStr, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	sp := strings.Fields(line)
	if len(sp) < 2 {
		return 0, 0, fmt.Errorf("benchutil: bad bucket line %q", line)
	}
	count, err = strconv.ParseFloat(sp[len(sp)-1], 64)
	return le, count, err
}
