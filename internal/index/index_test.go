package index

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestPostingsAndMatch(t *testing.T) {
	ix := New()
	ix.Add(1, "cpu", map[string]string{"host": "a", "region": "apac"})
	ix.Add(2, "cpu", map[string]string{"host": "b", "region": "apac"})
	ix.Add(3, "cpu", map[string]string{"host": "a", "region": "us"})
	ix.Add(4, "mem", map[string]string{"host": "a", "region": "apac"})

	cases := []struct {
		name     string
		matchers []Matcher
		want     []SeriesID
	}{
		{"metric only", []Matcher{{MetricName, "cpu"}}, []SeriesID{1, 2, 3}},
		{"one label", []Matcher{{"region", "apac"}}, []SeriesID{1, 2, 4}},
		{"metric + label", []Matcher{{MetricName, "cpu"}, {"region", "apac"}}, []SeriesID{1, 2}},
		{"three matchers", []Matcher{{MetricName, "cpu"}, {"region", "apac"}, {"host", "a"}}, []SeriesID{1}},
		{"no match", []Matcher{{MetricName, "cpu"}, {"region", "eu"}}, nil},
		{"unknown label", []Matcher{{"zone", "z1"}}, nil},
		{"no matchers", nil, nil},
	}
	for _, tc := range cases {
		got := ix.Match(tc.matchers...)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPostingsStaySorted(t *testing.T) {
	ix := New()
	// deliberately out of order
	for _, id := range []SeriesID{5, 1, 9, 3, 7} {
		ix.Add(id, "cpu", map[string]string{"host": "a"})
	}
	got := ix.Postings("host", "a")
	want := []SeriesID{1, 3, 5, 7, 9}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAddIsIdempotent(t *testing.T) {
	ix := New()
	ix.Add(1, "cpu", map[string]string{"host": "a"})
	ix.Add(1, "cpu", map[string]string{"host": "a"})
	if got := ix.Postings("host", "a"); len(got) != 1 {
		t.Fatalf("duplicate Add should not duplicate the posting: %v", got)
	}
}

func TestCardinality(t *testing.T) {
	ix := New()
	for id := SeriesID(1); id <= 100; id++ {
		ix.Add(id, "cpu", map[string]string{"host": fmt.Sprintf("h%d", id%10)})
	}
	if got := ix.Cardinality(MetricName, "cpu"); got != 100 {
		t.Errorf("metric cardinality = %d, want 100", got)
	}
	if got := ix.Cardinality("host", "h1"); got != 10 {
		t.Errorf("host=h1 cardinality = %d, want 10", got)
	}
}

func TestIntersectEdgeCases(t *testing.T) {
	if got := Intersect(nil); got != nil {
		t.Errorf("no lists: got %v", got)
	}
	if got := Intersect([][]SeriesID{{1, 2, 3}, {}}); got != nil {
		t.Errorf("empty list should empty the result: got %v", got)
	}
	if got := Intersect([][]SeriesID{{1, 2, 3}}); !reflect.DeepEqual(got, []SeriesID{1, 2, 3}) {
		t.Errorf("single list: got %v", got)
	}
	// disjoint
	if got := Intersect([][]SeriesID{{1, 3, 5}, {2, 4, 6}}); got != nil {
		t.Errorf("disjoint: got %v", got)
	}
	// identical
	if got := Intersect([][]SeriesID{{1, 2, 3}, {1, 2, 3}}); !reflect.DeepEqual(got, []SeriesID{1, 2, 3}) {
		t.Errorf("identical: got %v", got)
	}
}

// Galloping must be correct when list sizes are wildly skewed.
func TestIntersectSkewed(t *testing.T) {
	big := make([]SeriesID, 100000)
	for i := range big {
		big[i] = SeriesID(i)
	}
	small := []SeriesID{0, 50000, 99999}
	got := Intersect([][]SeriesID{big, small})
	if !reflect.DeepEqual(got, small) {
		t.Fatalf("got %v, want %v", got, small)
	}

	// a value past the end of the big list
	small2 := []SeriesID{7, 200000}
	got = Intersect([][]SeriesID{big, small2})
	if !reflect.DeepEqual(got, []SeriesID{7}) {
		t.Fatalf("got %v, want [7]", got)
	}
}

// Cross-check the fast intersection against a naive set-based one.
func TestIntersectFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	for round := 0; round < 500; round++ {
		nLists := r.Intn(4) + 1
		lists := make([][]SeriesID, nLists)
		for i := range lists {
			seen := map[SeriesID]bool{}
			n := r.Intn(60)
			for j := 0; j < n; j++ {
				seen[SeriesID(r.Intn(100))] = true
			}
			for id := range seen {
				lists[i] = append(lists[i], id)
			}
			sort.Slice(lists[i], func(a, b int) bool { return lists[i][a] < lists[i][b] })
		}

		want := naiveIntersect(lists)
		got := Intersect(lists)
		if len(got) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: got %v, want %v (inputs %v)", round, got, want, lists)
		}
	}
}

func naiveIntersect(lists [][]SeriesID) []SeriesID {
	if len(lists) == 0 {
		return nil
	}
	counts := map[SeriesID]int{}
	for _, l := range lists {
		for _, id := range l {
			counts[id]++
		}
	}
	var out []SeriesID
	for id, c := range counts {
		if c == len(lists) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Input lists must not be mutated: they alias the index's own storage.
func TestIntersectDoesNotMutateInputs(t *testing.T) {
	a := []SeriesID{1, 2, 3, 4, 5}
	b := []SeriesID{2, 4}
	aCopy := append([]SeriesID(nil), a...)
	bCopy := append([]SeriesID(nil), b...)

	Intersect([][]SeriesID{a, b})

	if !reflect.DeepEqual(a, aCopy) {
		t.Errorf("first list mutated: %v, was %v", a, aCopy)
	}
	if !reflect.DeepEqual(b, bCopy) {
		t.Errorf("second list mutated: %v, was %v", b, bCopy)
	}
}

func BenchmarkIntersect(b *testing.B) {
	build := func(n int, stride SeriesID) []SeriesID {
		l := make([]SeriesID, n)
		for i := range l {
			l[i] = SeriesID(i) * stride
		}
		return l
	}
	big := build(100000, 1)
	medium := build(10000, 10)
	tiny := build(10, 10000)

	b.Run("big+medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Intersect([][]SeriesID{big, medium})
		}
	})
	b.Run("big+tiny", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Intersect([][]SeriesID{big, tiny})
		}
	})
	b.Run("three lists", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Intersect([][]SeriesID{big, medium, tiny})
		}
	})
}

// intersectLinear is the baseline for galloping: a plain two-pointer merge
// that steps through both lists one element at a time.
func intersectLinear(a, b []SeriesID) []SeriesID {
	out := a[:0]
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

// intersectInOrder is the baseline for shortest-first ordering: lists are
// intersected in the order given.
func intersectInOrder(lists [][]SeriesID, two func(a, b []SeriesID) []SeriesID) []SeriesID {
	result := append([]SeriesID(nil), lists[0]...)
	for _, l := range lists[1:] {
		result = two(result, l)
	}
	return result
}

func buildPostings(n int, stride SeriesID) []SeriesID {
	l := make([]SeriesID, n)
	for i := range l {
		l[i] = SeriesID(i) * stride
	}
	return l
}

// BenchmarkIntersectOptimizations measures each optimisation against the naive
// version on the same input, so the speedup is like for like.
func BenchmarkIntersectOptimizations(b *testing.B) {
	big := buildPostings(100000, 1)
	medium := buildPostings(10000, 10)
	tiny := buildPostings(10, 10000)

	// The baselines must agree with Intersect, or the comparison means nothing.
	want := Intersect([][]SeriesID{big, medium, tiny})
	if got := intersectInOrder([][]SeriesID{big, medium, tiny}, intersectLinear); !reflect.DeepEqual(got, want) {
		b.Fatalf("baseline disagrees with Intersect: %v vs %v", got, want)
	}

	// 10 series against 100k: galloping skips most of the long list.
	b.Run("skewed/linear", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			intersectInOrder([][]SeriesID{tiny, big}, intersectLinear)
		}
	})
	b.Run("skewed/galloping", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Intersect([][]SeriesID{tiny, big})
		}
	})

	// Three lists given longest first: the naive version carries 100k IDs into
	// the first pass, while shortest-first starts from 10.
	b.Run("three/in-order-linear", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			intersectInOrder([][]SeriesID{big, medium, tiny}, intersectLinear)
		}
	})
	b.Run("three/shortest-first-galloping", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Intersect([][]SeriesID{big, medium, tiny})
		}
	})
}
