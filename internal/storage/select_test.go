package storage

import (
	"errors"
	"reflect"
	"testing"

	"minitsdb/internal/compress"
	"minitsdb/internal/index"
	"minitsdb/internal/model"
)

func metric(name string) index.Matcher { return index.Matcher{Name: index.MetricName, Value: name} }
func label(name, value string) index.Matcher {
	return index.Matcher{Name: name, Value: value}
}

func mustSelect(t *testing.T, s *MemoryStorage, start, end int64, matchers ...index.Matcher) []Series {
	t.Helper()
	got, err := s.Select(matchers, start, end)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	return got
}

// seriesHosts lists the host label of each returned series, in order.
func seriesHosts(series []Series) []string {
	hosts := make([]string, len(series))
	for i, s := range series {
		hosts[i] = s.Labels["host"]
	}
	return hosts
}

// A partial label set finds every series carrying it, whatever other labels
// those series have -- the query Query cannot answer.
func TestSelectByPartialLabels(t *testing.T) {
	s := newTestStore(t)
	for _, sm := range []model.Sample{
		{Metric: "cpu", Labels: model.Labels{"host": "a", "region": "apac"}, Point: model.Point{Timestamp: 10, Value: 1}},
		{Metric: "cpu", Labels: model.Labels{"host": "b", "region": "apac"}, Point: model.Point{Timestamp: 10, Value: 2}},
		{Metric: "cpu", Labels: model.Labels{"host": "c", "region": "us"}, Point: model.Point{Timestamp: 10, Value: 3}},
		{Metric: "mem", Labels: model.Labels{"host": "a", "region": "apac"}, Point: model.Point{Timestamp: 10, Value: 4}},
	} {
		mustAppend(t, s, sm)
	}

	cases := []struct {
		name     string
		matchers []index.Matcher
		want     []string
	}{
		{"metric and region", []index.Matcher{metric("cpu"), label("region", "apac")}, []string{"a", "b"}},
		{"metric only", []index.Matcher{metric("cpu")}, []string{"a", "b", "c"}},
		{"label across metrics", []index.Matcher{label("host", "a")}, []string{"a", "a"}},
		{"no match", []index.Matcher{metric("cpu"), label("region", "eu")}, []string{}},
		{"no matchers", nil, []string{}},
	}
	for _, tc := range cases {
		got := seriesHosts(mustSelect(t, s, 0, 100, tc.matchers...))
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got hosts %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The index outlives the head: series flushed to a block are still found,
// and their block samples merge with the samples written since.
func TestSelectSpansFlush(t *testing.T) {
	s := newTestStore(t) // flushes every 5 samples
	a := model.Labels{"host": "a", "region": "apac"}
	b := model.Labels{"host": "b", "region": "apac"}

	// 3 + 2 samples trigger a flush with both series in the block
	for ts := int64(1); ts <= 3; ts++ {
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: a, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}
	for ts := int64(1); ts <= 2; ts++ {
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: b, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}
	if s.BlockSeq != 1 || len(s.series) != 0 {
		t.Fatalf("expected one flush and an empty head, got BlockSeq=%d head=%d", s.BlockSeq, len(s.series))
	}
	// one more sample for a, now in the head
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: a, Point: model.Point{Timestamp: 4, Value: 4}})

	got := mustSelect(t, s, 0, 100, metric("cpu"), label("region", "apac"))
	if hosts := seriesHosts(got); !reflect.DeepEqual(hosts, []string{"a", "b"}) {
		t.Fatalf("got hosts %v, want [a b]", hosts)
	}
	wantA := []model.Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}, {Timestamp: 4, Value: 4}}
	if !reflect.DeepEqual(got[0].Points, wantA) {
		t.Errorf("series a: got %v, want %v", got[0].Points, wantA)
	}
	if len(got[1].Points) != 2 {
		t.Errorf("series b: got %d points, want 2", len(got[1].Points))
	}
}

// A series with nothing in the requested range is not returned at all.
func TestSelectOmitsSeriesOutsideRange(t *testing.T) {
	s := newTestStore(t)
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: model.Labels{"host": "a"}, Point: model.Point{Timestamp: 10, Value: 1}})
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: model.Labels{"host": "b"}, Point: model.Point{Timestamp: 500, Value: 2}})

	got := mustSelect(t, s, 0, 100, metric("cpu"))
	if hosts := seriesHosts(got); !reflect.DeepEqual(hosts, []string{"a"}) {
		t.Fatalf("got hosts %v, want [a]", hosts)
	}
}

// Returned labels are a copy: a caller editing them must not corrupt the store.
func TestSelectReturnsLabelCopy(t *testing.T) {
	s := newTestStore(t)
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: model.Labels{"host": "a"}, Point: model.Point{Timestamp: 10, Value: 1}})

	got := mustSelect(t, s, 0, 100, metric("cpu"))
	got[0].Labels["host"] = "tampered"

	again := mustSelect(t, s, 0, 100, metric("cpu"))
	if again[0].Labels["host"] != "a" {
		t.Fatalf("store labels were mutated through a Select result: %v", again[0].Labels)
	}
}

// Regression: ordering used to be checked against the head chunk, which a
// flush empties, so a sample older than already-flushed data was accepted and
// queries returned two values for one timestamp.
func TestOrderingSurvivesFlush(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"host": "a"}
	for ts := int64(1); ts <= 5; ts++ { // the fifth sample triggers a flush
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}

	older := model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 3, Value: 99}}
	if err := s.Append(older); !errors.Is(err, compress.ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder after flush, got %v", err)
	}
	if got := mustQuery(t, s, "cpu", labels, 0, 100); len(got) != 5 {
		t.Fatalf("expected the original 5 points, got %d: %v", len(got), got)
	}
}
