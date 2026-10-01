package query

import (
	"math/rand"
	"reflect"
	"testing"

	"minitsdb/internal/model"
)

func pts(pairs ...float64) []model.Point {
	out := make([]model.Point, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, model.Point{Timestamp: int64(pairs[i]), Value: pairs[i+1]})
	}
	return out
}

func TestRollupReducers(t *testing.T) {
	// two buckets of width 60: [0,60) and [60,120)
	in := pts(0, 10, 20, 12, 40, 14, 60, 30, 80, 32)

	cases := []struct {
		rollup Rollup
		want   []model.Point
	}{
		{RollupAvg, pts(0, 12, 60, 31)},
		{RollupSum, pts(0, 36, 60, 62)},
		{RollupMin, pts(0, 10, 60, 30)},
		{RollupMax, pts(0, 14, 60, 32)},
		{RollupCount, pts(0, 3, 60, 2)},
		{RollupLast, pts(0, 14, 60, 32)},
	}
	for _, tc := range cases {
		if got := RollupPoints(in, 60, tc.rollup); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.rollup, got, tc.want)
		}
	}
}

// Buckets align to multiples of step, not to the first sample, so the same
// sample lands in the same bucket whatever range is queried.
func TestBucketsAlignToStep(t *testing.T) {
	got := RollupPoints(pts(95, 1, 130, 2, 175, 3), 60, RollupSum)
	want := pts(60, 1, 120, 5)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestBucketStartNegative(t *testing.T) {
	for _, tc := range []struct{ ts, want int64 }{
		{0, 0}, {59, 0}, {60, 60}, {-1, -60}, {-60, -60}, {-61, -120},
	} {
		if got := bucketStart(tc.ts, 60); got != tc.want {
			t.Errorf("bucketStart(%d, 60) = %d, want %d", tc.ts, got, tc.want)
		}
	}
}

// Empty buckets are left out rather than emitted as zeros.
func TestRollupSkipsEmptyBuckets(t *testing.T) {
	got := RollupPoints(pts(0, 1, 600, 2), 60, RollupAvg)
	if want := pts(0, 1, 600, 2); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := RollupPoints(nil, 60, RollupAvg); len(got) != 0 {
		t.Fatalf("no input should give no buckets, got %v", got)
	}
}

func TestRate(t *testing.T) {
	// a counter rising by 6 every 10s: 0.6 per second
	in := pts(0, 0, 10, 6, 20, 12, 30, 18, 40, 24, 50, 30, 60, 36, 70, 42)
	got := RollupPoints(in, 60, RollupRate)
	// bucket 0 gets the increases credited to samples at 10..50 (5 x 6 = 30);
	// bucket 60 gets those at 60 and 70 (2 x 6 = 12)
	want := pts(0, 30.0/60, 60, 12.0/60)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// The increase between the last sample of one bucket and the first of the
// next belongs to the later bucket; it must not vanish at the boundary.
func TestRateCountsIncreaseAcrossBoundary(t *testing.T) {
	got := RollupPoints(pts(50, 100, 70, 160), 60, RollupRate)
	if want := pts(60, 60.0/60); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// A counter that drops has restarted from zero. Its new value is the increase
// since the reset, not a negative rate.
func TestRateHandlesCounterReset(t *testing.T) {
	// 100 -> 150 (+50), reset, -> 20 (+20), -> 50 (+30)
	got := RollupPoints(pts(0, 100, 10, 150, 20, 20, 30, 50), 60, RollupRate)
	if want := pts(0, 100.0/60); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, p := range got {
		if p.Value < 0 {
			t.Fatalf("rate must never be negative, got %v", got)
		}
	}
}

func TestRateNeedsTwoSamples(t *testing.T) {
	if got := RollupPoints(pts(10, 5), 60, RollupRate); len(got) != 0 {
		t.Fatalf("a single sample has no increase, got %v", got)
	}
}

func TestAggregateAcrossSeries(t *testing.T) {
	series := []Series{
		{Labels: model.Labels{"host": "a", "region": "apac"}, Points: pts(0, 12, 60, 31)},
		{Labels: model.Labels{"host": "b", "region": "apac"}, Points: pts(0, 20, 60, 25)},
		{Labels: model.Labels{"host": "c", "region": "us"}, Points: pts(0, 5)},
	}

	cases := []struct {
		agg  Aggregator
		want []model.Point
	}{
		{AggSum, pts(0, 37, 60, 56)},
		{AggAvg, pts(0, 37.0/3, 60, 28)},
		{AggMin, pts(0, 5, 60, 25)},
		{AggMax, pts(0, 20, 60, 31)},
		{AggCount, pts(0, 3, 60, 2)}, // host c has no value at 60
	}
	for _, tc := range cases {
		got := Aggregate(series, tc.agg, nil)
		if len(got) != 1 {
			t.Fatalf("%s: got %d groups, want 1", tc.agg, len(got))
		}
		if len(got[0].Labels) != 0 {
			t.Errorf("%s: ungrouped output should carry no labels, got %v", tc.agg, got[0].Labels)
		}
		if !reflect.DeepEqual(got[0].Points, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.agg, got[0].Points, tc.want)
		}
	}
}

func TestAggregateBy(t *testing.T) {
	series := []Series{
		{Labels: model.Labels{"host": "a", "region": "apac"}, Points: pts(0, 1)},
		{Labels: model.Labels{"host": "b", "region": "apac"}, Points: pts(0, 2)},
		{Labels: model.Labels{"host": "c", "region": "us"}, Points: pts(0, 4)},
	}
	got := Aggregate(series, AggSum, []string{"region"})
	want := []Series{
		{Labels: model.Labels{"region": "apac"}, Points: pts(0, 3)},
		{Labels: model.Labels{"region": "us"}, Points: pts(0, 4)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// Rolling up must never lose or invent samples: summing the count rollup
// over all buckets gives back the input length, for any step.
func TestRollupCountConservesSamples(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for round := 0; round < 200; round++ {
		n := r.Intn(200)
		in := make([]model.Point, n)
		ts := int64(r.Intn(2000) - 1000) // include negative timestamps
		for i := range in {
			ts += int64(r.Intn(30) + 1)
			in[i] = model.Point{Timestamp: ts, Value: r.Float64()}
		}
		step := int64(r.Intn(100) + 1)

		total := 0.0
		prev := int64(-1 << 62)
		for _, p := range RollupPoints(in, step, RollupCount) {
			if p.Timestamp <= prev {
				t.Fatalf("round %d: buckets not strictly increasing: %d after %d", round, p.Timestamp, prev)
			}
			if p.Timestamp%step != 0 {
				t.Fatalf("round %d: bucket %d not aligned to step %d", round, p.Timestamp, step)
			}
			prev = p.Timestamp
			total += p.Value
		}
		if int(total) != n {
			t.Fatalf("round %d: buckets hold %v samples, input had %d", round, total, n)
		}
	}
}

func TestParse(t *testing.T) {
	if r, err := ParseRollup(""); err != nil || r != RollupAvg {
		t.Errorf("empty rollup should default to avg, got %q %v", r, err)
	}
	if _, err := ParseRollup("median"); err == nil {
		t.Error("unknown rollup should be rejected")
	}
	if a, err := ParseAggregator(""); err != nil || a != "" {
		t.Errorf("empty aggregator should mean none, got %q %v", a, err)
	}
	if _, err := ParseAggregator("p99"); err == nil {
		t.Error("unknown aggregator should be rejected")
	}
}
