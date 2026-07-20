package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"minitsdb/internal/model"
	"minitsdb/internal/storage"
)

type Handler struct {
	store *storage.MemoryStorage
}

func NewHandler(store *storage.MemoryStorage) *Handler {
	return &Handler{store: store}
}

func(h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /write", h.HandleWrite)
	mux.HandleFunc("GET /query", h.HandleQuery)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	metric := q.Get("metric")
	if metric == "" {
		http.Error(w, "metric query parameter is required", http.StatusBadRequest)
		return
	}

	start := parseIntDefault(q.Get("start"), 0)
	end := parseIntDefault(q.Get("end"), 1 << 62)

	labels := model.Labels{}
	for key, values := range q {
		if key == "metric" || key == "start" || key == "end" {
			continue
		}
		if len(values) > 0 {
			labels[key] = values[0]
		}
	}

	points := h.store.Query(metric, labels, start, end)
	
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func parseIntDefault(s string, def int64) int64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}
