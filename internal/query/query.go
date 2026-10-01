// Package query evaluates time-series queries the way Datadog does: each
// series is first rolled up into fixed-width time buckets, and the rolled-up
// series can then be aggregated across series, optionally grouped by labels.
package query

import (
	"fmt"
	"sort"

	"minitsdb/internal/model"
)

// Rollup reduces the samples of one series that fall in one time bucket to a
// single value.
type Rollup string

const (
	RollupAvg   Rollup = "avg"
	RollupSum   Rollup = "sum"
	RollupMin   Rollup = "min"
	RollupMax   Rollup = "max"
	RollupCount Rollup = "count"
	RollupLast  Rollup = "last"
	// RollupRate is the per-unit-time increase of a counter over the bucket.
	// A drop in value is treated as a counter reset, not a decrease.
	RollupRate Rollup = "rate"
)

// Aggregator combines the rolled-up values of several series in one bucket.
type Aggregator string

const (
	AggSum   Aggregator = "sum"
	AggAvg   Aggregator = "avg"
	AggMin   Aggregator = "min"
	AggMax   Aggregator = "max"
	AggCount Aggregator = "count"
)

// ParseRollup validates a rollup name; the empty string means avg, Datadog's
// default.
func ParseRollup(s string) (Rollup, error) {
	switch r := Rollup(s); r {
	case "":
		return RollupAvg, nil
	case RollupAvg, RollupSum, RollupMin, RollupMax, RollupCount, RollupLast, RollupRate:
		return r, nil
	}
	return "", fmt.Errorf("unknown rollup %q", s)
}

// ParseAggregator validates an aggregator name; the empty string means no
// aggregation across series.
func ParseAggregator(s string) (Aggregator, error) {
	switch a := Aggregator(s); a {
	case "", AggSum, AggAvg, AggMin, AggMax, AggCount:
		return a, nil
	}
	return "", fmt.Errorf("unknown aggregator %q", s)
}

// Series is one input or output series of a query. Aggregated series have no
// metric: once series are combined, the result is no longer any one metric.
type Series struct {
	Metric string        `json:"metric,omitempty"`
	Labels model.Labels  `json:"labels"`
	Points []model.Point `json:"points"`
}

// bucketStart aligns ts down to a multiple of step. Aligning to absolute
// multiples rather than to the query's start means a sample lands in the same
// bucket however the query range is chosen.
func bucketStart(ts, step int64) int64 {
	b := ts - ts%step
	if ts%step < 0 { // % truncates toward zero; floor needs one step lower
		b -= step
	}
	return b
}

// RollupPoints buckets time-sorted points by step and reduces each bucket.
// Only buckets that receive data are returned, so the output is never larger
// than the input however wide the query range is.
func RollupPoints(points []model.Point, step int64, r Rollup) []model.Point {
	if r == RollupRate {
		return ratePoints(points, step)
	}

	var out []model.Point
	for i := 0; i < len(points); {
		b := bucketStart(points[i].Timestamp, step)
		j := i
		for j < len(points) && bucketStart(points[j].Timestamp, step) == b {
			j++
		}
		out = append(out, model.Point{Timestamp: b, Value: reduce(points[i:j], r)})
		i = j
	}
	return out
}

func reduce(points []model.Point, r Rollup) float64 {
	switch r {
	case RollupCount:
		return float64(len(points))
	case RollupLast:
		return points[len(points)-1].Value
	}
	acc := points[0].Value
	for _, p := range points[1:] {
		switch r {
		case RollupMin:
			acc = min(acc, p.Value)
		case RollupMax:
			acc = max(acc, p.Value)
		default: // sum and avg
			acc += p.Value
		}
	}
	if r == RollupAvg {
		acc /= float64(len(points))
	}
	return acc
}

// ratePoints computes each bucket's increase divided by step. The increase
// between two consecutive samples is credited to the later sample's bucket,
// so increases that straddle a bucket boundary are not lost. The first sample
// has no predecessor, so it contributes nothing.
func ratePoints(points []model.Point, step int64) []model.Point {
	var out []model.Point
	for i := 1; i < len(points); i++ {
		delta := points[i].Value - points[i-1].Value
		if delta < 0 {
			// The counter restarted from zero, so everything it holds now
			// was counted since the reset.
			delta = points[i].Value
		}
		b := bucketStart(points[i].Timestamp, step)
		if n := len(out); n > 0 && out[n-1].Timestamp == b {
			out[n-1].Value += delta
		} else {
			out = append(out, model.Point{Timestamp: b, Value: delta})
		}
	}
	for i := range out {
		out[i].Value /= float64(step)
	}
	return out
}

// Aggregate combines rolled-up series bucket by bucket. Series are grouped by
// the values of the by labels; with no by labels, all series form one group.
// A group's output carries only its by labels, and a bucket is computed from
// whichever series in the group have a value there.
func Aggregate(series []Series, agg Aggregator, by []string) []Series {
	type group struct {
		labels  model.Labels
		buckets map[int64][]float64
	}
	groups := map[string]*group{}
	for _, s := range series {
		if len(s.Points) == 0 {
			continue // contributes nothing, and must not create an empty group
		}
		labels := model.Labels{}
		for _, name := range by {
			if v, ok := s.Labels[name]; ok {
				labels[name] = v
			}
		}
		key := model.SeriesKey("", labels)
		g := groups[key]
		if g == nil {
			g = &group{labels: labels, buckets: map[int64][]float64{}}
			groups[key] = g
		}
		for _, p := range s.Points {
			g.buckets[p.Timestamp] = append(g.buckets[p.Timestamp], p.Value)
		}
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]Series, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		points := make([]model.Point, 0, len(g.buckets))
		for ts, values := range g.buckets {
			points = append(points, model.Point{Timestamp: ts, Value: combine(values, agg)})
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
		out = append(out, Series{Labels: g.labels, Points: points})
	}
	return out
}

func combine(values []float64, agg Aggregator) float64 {
	if agg == AggCount {
		return float64(len(values))
	}
	acc := values[0]
	for _, v := range values[1:] {
		switch agg {
		case AggMin:
			acc = min(acc, v)
		case AggMax:
			acc = max(acc, v)
		default: // sum and avg
			acc += v
		}
	}
	if agg == AggAvg {
		acc /= float64(len(values))
	}
	return acc
}
