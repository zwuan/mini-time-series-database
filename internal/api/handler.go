package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"minitsdb/internal/compress"
	"minitsdb/internal/index"
	"minitsdb/internal/model"
	"minitsdb/internal/query"
	"minitsdb/internal/storage"
)

type Handler struct {
	store *storage.MemoryStorage
}

func NewHandler(store *storage.MemoryStorage) *Handler {
	return &Handler{store: store}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /write", h.HandleWrite)
	mux.HandleFunc("GET /query", h.HandleQuery)
	mux.HandleFunc("GET /select", h.HandleSelect)
	mux.HandleFunc("GET /aggregate", h.HandleAggregate)
}

func (h *Handler) HandleWrite(w http.ResponseWriter, r *http.Request) {
	var sample model.Sample

	if err := json.NewDecoder(r.Body).Decode(&sample); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if sample.Metric == "" {
		http.Error(w, "metric name is required", http.StatusBadRequest)
		return
	}

	if err := h.store.Append(sample); err != nil {
		if errors.Is(err, compress.ErrOutOfOrder) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	sel, err := parseSelector(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if sel.metric == "" {
		http.Error(w, "metric query parameter is required", http.StatusBadRequest)
		return
	}

	points, err := h.store.Query(sel.metric, sel.labels, sel.start, sel.end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, points)
}

func (h *Handler) HandleSelect(w http.ResponseWriter, r *http.Request) {
	sel, err := parseSelector(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	matchers := sel.matchers()
	// An unconstrained select would return every series in the store.
	if len(matchers) == 0 {
		http.Error(w, "at least one of metric or a label is required", http.StatusBadRequest)
		return
	}

	series, err := h.store.Select(matchers, sel.start, sel.end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, series)
}

// aggregateParams are the query parameters /aggregate reads for itself, so
// they are not mistaken for label matchers.
var aggregateParams = []string{"step", "rollup", "agg", "by"}

// HandleAggregate rolls each matching series up into step-wide time buckets
// and, when agg is given, combines the rolled-up series bucket by bucket,
// optionally grouped by the labels listed in by. It is the same two-stage
// model as a Datadog query such as
// sum:cpu{region:apac} by {host}.rollup(avg, 60).
func (h *Handler) HandleAggregate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sel, err := parseSelector(q, aggregateParams...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	step, err := parsePositiveInt(q, "step")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rollup, err := query.ParseRollup(q.Get("rollup"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	agg, err := query.ParseAggregator(q.Get("agg"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var by []string
	for _, name := range strings.Split(q.Get("by"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			by = append(by, name)
		}
	}
	if len(by) > 0 && agg == "" {
		http.Error(w, "by groups the output of agg, so agg is required", http.StatusBadRequest)
		return
	}

	matchers := sel.matchers()
	if len(matchers) == 0 {
		http.Error(w, "at least one of metric or a label is required", http.StatusBadRequest)
		return
	}
	series, err := h.store.Select(matchers, sel.start, sel.end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result := make([]query.Series, 0, len(series))
	for _, s := range series {
		points := query.RollupPoints(s.Points, step, rollup)
		if len(points) == 0 {
			continue // e.g. a rate over a single sample
		}
		result = append(result, query.Series{Metric: s.Metric, Labels: s.Labels, Points: points})
	}
	if agg != "" {
		result = query.Aggregate(result, agg, by)
	}
	writeJSON(w, result)
}

type selector struct {
	metric     string
	labels     model.Labels
	start, end int64
}

// matchers turns the selector into index matchers, the metric being matched
// as the __name__ label.
func (sel selector) matchers() []index.Matcher {
	matchers := make([]index.Matcher, 0, len(sel.labels)+1)
	if sel.metric != "" {
		matchers = append(matchers, index.Matcher{Name: index.MetricName, Value: sel.metric})
	}
	for name, value := range sel.labels {
		matchers = append(matchers, index.Matcher{Name: name, Value: value})
	}
	return matchers
}

// parseSelector reads metric, start and end, and treats every other query
// parameter as a label, except the reserved ones an endpoint reads itself.
func parseSelector(q url.Values, reserved ...string) (selector, error) {
	sel := selector{metric: q.Get("metric"), labels: model.Labels{}}

	var err error
	if sel.start, err = parseTimestamp(q, "start", 0); err != nil {
		return selector{}, err
	}
	if sel.end, err = parseTimestamp(q, "end", math.MaxInt64); err != nil {
		return selector{}, err
	}

	for key, values := range q {
		if key == "metric" || key == "start" || key == "end" || slices.Contains(reserved, key) {
			continue
		}
		if len(values) > 0 {
			sel.labels[key] = values[0]
		}
	}
	return sel, nil
}

func parseTimestamp(q url.Values, name string, def int64) (int64, error) {
	s := q.Get(name)
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer timestamp, got %q", name, s)
	}
	return v, nil
}

func parsePositiveInt(q url.Values, name string) (int64, error) {
	s := q.Get(name)
	if s == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, s)
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
