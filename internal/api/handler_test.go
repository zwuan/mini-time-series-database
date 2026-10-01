package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"minitsdb/internal/model"
	"minitsdb/internal/query"
	"minitsdb/internal/storage"
)

// newTestServer wires a Handler to a fresh store and returns its mux.
func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	store, err := storage.NewMemoryStorage(filepath.Join(t.TempDir(), "wal.log"), 1000)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	mux := http.NewServeMux()
	NewHandler(store).Register(mux)
	return mux
}

func do(t *testing.T, srv http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func write(t *testing.T, srv http.Handler, body string) {
	t.Helper()
	if rec := do(t, srv, http.MethodPost, "/write", body); rec.Code != http.StatusNoContent {
		t.Fatalf("write %s: status %d, body %q", body, rec.Code, rec.Body.String())
	}
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
	}
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func seed(t *testing.T, srv http.Handler) {
	t.Helper()
	write(t, srv, `{"metric":"cpu","labels":{"host":"a","region":"apac"},"point":{"timestamp":10,"value":1}}`)
	write(t, srv, `{"metric":"cpu","labels":{"host":"b","region":"apac"},"point":{"timestamp":10,"value":2}}`)
	write(t, srv, `{"metric":"cpu","labels":{"host":"c","region":"us"},"point":{"timestamp":10,"value":3}}`)
}

func TestSelectMatchesPartialLabels(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	got := decode[[]storage.Series](t, do(t, srv, http.MethodGet, "/select?metric=cpu&region=apac", ""))
	if len(got) != 2 {
		t.Fatalf("got %d series, want 2: %+v", len(got), got)
	}
	want := []storage.Series{
		{Metric: "cpu", Labels: model.Labels{"host": "a", "region": "apac"}, Points: []model.Point{{Timestamp: 10, Value: 1}}},
		{Metric: "cpu", Labels: model.Labels{"host": "b", "region": "apac"}, Points: []model.Point{{Timestamp: 10, Value: 2}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// The metric is optional for /select: a label alone is enough.
func TestSelectWithoutMetric(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	got := decode[[]storage.Series](t, do(t, srv, http.MethodGet, "/select?region=us", ""))
	if len(got) != 1 || got[0].Labels["host"] != "c" {
		t.Fatalf("got %+v, want only host c", got)
	}
}

// /query and /select answer different questions for the same parameters:
// /query wants the series whose labels are exactly these, /select wants every
// series that has at least these.
func TestQueryIsExactSelectIsSubset(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	points := decode[[]model.Point](t, do(t, srv, http.MethodGet, "/query?metric=cpu&host=a", ""))
	if len(points) != 0 {
		t.Fatalf("/query should not match cpu{host=a,region=apac} with host=a alone, got %v", points)
	}

	series := decode[[]storage.Series](t, do(t, srv, http.MethodGet, "/select?metric=cpu&host=a", ""))
	if len(series) != 1 {
		t.Fatalf("/select should match cpu{host=a,region=apac} with host=a, got %+v", series)
	}
}

// An empty result is an empty JSON array, not null, so clients can iterate it.
func TestSelectEmptyResultIsArray(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	rec := do(t, srv, http.MethodGet, "/select?metric=does_not_exist", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
		t.Fatalf("body = %q, want []", body)
	}
}

func TestRequestErrors(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)

	cases := []struct {
		name   string
		method string
		target string
		body   string
		want   int
	}{
		{"select with no matchers", http.MethodGet, "/select", "", http.StatusBadRequest},
		{"select with only a time range", http.MethodGet, "/select?start=0&end=100", "", http.StatusBadRequest},
		{"non-integer start", http.MethodGet, "/select?metric=cpu&start=yesterday", "", http.StatusBadRequest},
		{"non-integer end", http.MethodGet, "/query?metric=cpu&host=a&end=1e9", "", http.StatusBadRequest},
		{"query without metric", http.MethodGet, "/query?host=a", "", http.StatusBadRequest},
		{"invalid JSON", http.MethodPost, "/write", "not json", http.StatusBadRequest},
		{"missing metric", http.MethodPost, "/write", `{"labels":{"host":"a"},"point":{"timestamp":1,"value":1}}`, http.StatusBadRequest},
		{"out of order", http.MethodPost, "/write", `{"metric":"cpu","labels":{"host":"a","region":"apac"},"point":{"timestamp":5,"value":9}}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		if rec := do(t, srv, tc.method, tc.target, tc.body); rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d (body %q)", tc.name, rec.Code, tc.want, rec.Body.String())
		}
	}
}

func writePoint(t *testing.T, srv http.Handler, metric, labels string, ts int64, value float64) {
	t.Helper()
	write(t, srv, fmt.Sprintf(`{"metric":%q,"labels":%s,"point":{"timestamp":%d,"value":%v}}`, metric, labels, ts, value))
}

// seedGauges writes two buckets of a cpu gauge for three hosts in two regions.
func seedGauges(t *testing.T, srv http.Handler) {
	t.Helper()
	for _, s := range []struct {
		labels string
		values [4]float64 // at t=0, 30 (bucket 0) and t=60, 90 (bucket 60)
	}{
		{`{"host":"a","region":"apac"}`, [4]float64{10, 14, 30, 32}},
		{`{"host":"b","region":"apac"}`, [4]float64{20, 20, 24, 26}},
		{`{"host":"c","region":"us"}`, [4]float64{5, 7, 9, 11}},
	} {
		for i, ts := range []int64{0, 30, 60, 90} {
			writePoint(t, srv, "cpu", s.labels, ts, s.values[i])
		}
	}
}

// Without agg, every matching series comes back on its own, rolled up.
func TestAggregateRollupOnly(t *testing.T) {
	srv := newTestServer(t)
	seedGauges(t, srv)

	got := decode[[]query.Series](t, do(t, srv, http.MethodGet, "/aggregate?metric=cpu&region=apac&step=60&rollup=max", ""))
	want := []query.Series{
		{Metric: "cpu", Labels: model.Labels{"host": "a", "region": "apac"}, Points: []model.Point{{Timestamp: 0, Value: 14}, {Timestamp: 60, Value: 32}}},
		{Metric: "cpu", Labels: model.Labels{"host": "b", "region": "apac"}, Points: []model.Point{{Timestamp: 0, Value: 20}, {Timestamp: 60, Value: 26}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// sum:cpu{*} by {region}.rollup(avg, 60)
func TestAggregateSumByRegion(t *testing.T) {
	srv := newTestServer(t)
	seedGauges(t, srv)

	got := decode[[]query.Series](t, do(t, srv, http.MethodGet, "/aggregate?metric=cpu&step=60&rollup=avg&agg=sum&by=region", ""))
	want := []query.Series{
		// apac: avg(a) + avg(b) = 12+20 at 0, 31+25 at 60
		{Labels: model.Labels{"region": "apac"}, Points: []model.Point{{Timestamp: 0, Value: 32}, {Timestamp: 60, Value: 56}}},
		{Labels: model.Labels{"region": "us"}, Points: []model.Point{{Timestamp: 0, Value: 6}, {Timestamp: 60, Value: 10}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// Aggregated output carries no metric, so the JSON omits the field entirely.
func TestAggregatedOutputOmitsMetric(t *testing.T) {
	srv := newTestServer(t)
	seedGauges(t, srv)

	rec := do(t, srv, http.MethodGet, "/aggregate?metric=cpu&step=60&agg=avg", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"metric"`) {
		t.Fatalf("aggregated output should not name a metric: %s", rec.Body.String())
	}
}

// rate over a counter, including a reset partway through.
func TestAggregateRate(t *testing.T) {
	srv := newTestServer(t)
	// +60 per 10s, then the counter restarts at 30
	for _, p := range [][2]float64{{0, 0}, {10, 60}, {20, 120}, {30, 30}, {40, 90}} {
		writePoint(t, srv, "requests", `{"host":"a"}`, int64(p[0]), p[1])
	}

	got := decode[[]query.Series](t, do(t, srv, http.MethodGet, "/aggregate?metric=requests&step=60&rollup=rate", ""))
	if len(got) != 1 || len(got[0].Points) != 1 {
		t.Fatalf("got %+v, want one series with one bucket", got)
	}
	// increases: 60, 60, 30 (reset), 60 = 210 over a 60s bucket
	if v := got[0].Points[0].Value; v != 210.0/60 {
		t.Fatalf("rate = %v, want %v", v, 210.0/60)
	}
}

func TestAggregateEmptyResultIsArray(t *testing.T) {
	srv := newTestServer(t)
	seedGauges(t, srv)

	rec := do(t, srv, http.MethodGet, "/aggregate?metric=does_not_exist&step=60&agg=sum", "")
	if body := strings.TrimSpace(rec.Body.String()); rec.Code != http.StatusOK || body != "[]" {
		t.Fatalf("status %d body %q, want 200 []", rec.Code, body)
	}
}

func TestAggregateRequestErrors(t *testing.T) {
	srv := newTestServer(t)
	seedGauges(t, srv)

	for _, tc := range []struct {
		name   string
		target string
	}{
		{"missing step", "/aggregate?metric=cpu"},
		{"zero step", "/aggregate?metric=cpu&step=0"},
		{"non-integer step", "/aggregate?metric=cpu&step=1m"},
		{"unknown rollup", "/aggregate?metric=cpu&step=60&rollup=median"},
		{"unknown agg", "/aggregate?metric=cpu&step=60&agg=p99"},
		{"by without agg", "/aggregate?metric=cpu&step=60&by=region"},
		{"no matchers", "/aggregate?step=60&agg=sum"},
	} {
		if rec := do(t, srv, http.MethodGet, tc.target, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400 (body %q)", tc.name, rec.Code, rec.Body.String())
		}
	}
}
