package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"

	"minitsdb/internal/compress"
	"minitsdb/internal/index"
	"minitsdb/internal/model"
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

	matchers := make([]index.Matcher, 0, len(sel.labels)+1)
	if sel.metric != "" {
		matchers = append(matchers, index.Matcher{Name: index.MetricName, Value: sel.metric})
	}
	for name, value := range sel.labels {
		matchers = append(matchers, index.Matcher{Name: name, Value: value})
	}
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

type selector struct {
	metric     string
	labels     model.Labels
	start, end int64
}

func parseSelector(q url.Values) (selector, error) {
	sel := selector{metric: q.Get("metric"), labels: model.Labels{}}

	var err error
	if sel.start, err = parseTimestamp(q, "start", 0); err != nil {
		return selector{}, err
	}
	if sel.end, err = parseTimestamp(q, "end", math.MaxInt64); err != nil {
		return selector{}, err
	}

	for key, values := range q {
		if key == "metric" || key == "start" || key == "end" {
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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
