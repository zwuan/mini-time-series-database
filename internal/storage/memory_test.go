package storage

import (
	"os"
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

// Writing flushThreshold samples should produce one block, clear the head and
// truncate the WAL, while Query still reads all data back from disk.
func TestFlushToBlockAndQueryMerge(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"host": "a"}

	// flushThreshold defaults to 5: writing 5 samples triggers one flush.
	for i := 1; i <= 5; i++ {
		mustAppend(t, s, model.Sample{
			Metric: "cpu",
			Labels: labels,
			Point:  model.Point{Timestamp: int64(i), Value: float64(i)},
		})
	}

	// The head should be cleared, pointCount reset, and one block produced.
	if len(s.series) != 0 {
		t.Fatalf("expected head cleared after flush, got %d series", len(s.series))
	}
	if s.pointCount != 0 {
		t.Fatalf("expected pointCount reset to 0, got %d", s.pointCount)
	}
	if s.BlockSeq != 1 {
		t.Fatalf("expected BlockSeq=1 after one flush, got %d", s.BlockSeq)
	}

	// The WAL should be truncated to an empty file.
	fi, err := os.Stat(s.walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if fi.Size() != 0 {
		t.Fatalf("expected WAL truncated to 0 bytes, got %d", fi.Size())
	}

	// Data moved from memory to disk, but Query should still read all 5 points.
	got := s.Query("cpu", labels, 0, 100)
	if len(got) != 5 {
		t.Fatalf("expected 5 points from block, got %d", len(got))
	}
	for i, p := range got {
		if p.Timestamp != int64(i+1) {
			t.Errorf("point %d: expected ts=%d, got %d", i, i+1, p.Timestamp)
		}
	}
}

// When data spans a flushed block and the still-in-memory head, Query merges both.
func TestQueryMergesBlockAndHead(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"host": "a"}

	// first 5 -> flushed into a block
	for i := 1; i <= 5; i++ {
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: int64(i), Value: float64(i)}})
	}
	// 2 more -> stay in the head
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 6, Value: 6}})
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 7, Value: 7}})

	got := s.Query("cpu", labels, 0, 100)
	if len(got) != 7 {
		t.Fatalf("expected 7 merged points, got %d", len(got))
	}
	// result must be sorted by time
	for i := 1; i < len(got); i++ {
		if got[i-1].Timestamp > got[i].Timestamp {
			t.Fatalf("result not sorted at %d: %v", i, got)
		}
	}
}

// After a restart, flushed data lives in blocks and unflushed data in the WAL;
// Query must read both back, and BlockSeq must continue from existing blocks.
func TestRestartRecoversBlocksAndWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	labels := model.Labels{"host": "a"}

	s, err := NewMemoryStorage(walPath)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	// 5 into a block, 2 more left in the WAL
	for i := 1; i <= 7; i++ {
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: int64(i), Value: float64(i)}})
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// restart
	s2, err := NewMemoryStorage(walPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer s2.Close()

	if s2.BlockSeq != 1 {
		t.Fatalf("expected BlockSeq continued at 1 after restart, got %d", s2.BlockSeq)
	}
	got := s2.Query("cpu", labels, 0, 100)
	if len(got) != 7 {
		t.Fatalf("expected 7 points after restart, got %d", len(got))
	}
}