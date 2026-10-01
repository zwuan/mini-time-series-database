package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"minitsdb/internal/model"
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
