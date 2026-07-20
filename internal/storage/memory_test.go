package storage

import (
	"path/filepath"
	"testing"

	"minitsdb/internal/model"
)

func newTestStore(t *testing.T) *MemoryStorage {
	t.Helper()
	walPath := filepath.Join(t.TempDir(), "test.wal")
	s, err := NewMemoryStorage(walPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	return s
}

func TestAppendAndRangeQuery(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"region": "apac"}

	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 300, Value: 3.0}})
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 100, Value: 1.0}})
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 200, Value: 2.0}})

	got := s.Query("cpu", labels, 150, 250)
	if len(got) != 1 {
		t.Fatalf("expected 1 point, got %d", len(got))
	}
	if got[0].Value != 2.0 {
		t.Errorf("expected value=2.0, got %v", got[0].Value)
	}
}

func TestQueryMissingSeries(t *testing.T) {
	s := newTestStore(t)
	got := s.Query("does_not_exist", nil, 0, 1000)
	if len(got) != 0 {
		t.Errorf("missing series should return empty, got %d points", len(got))
	}
}

func mustAppend(t *testing.T, s *MemoryStorage, sample model.Sample) {
	t.Helper()
	if err := s.Append(sample); err != nil {
		t.Fatalf("append failed: %v", err)
	}
}