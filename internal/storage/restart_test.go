package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"minitsdb/internal/compress"
	"minitsdb/internal/model"
)

// reopen closes s and opens a new store over the same WAL and blocks.
func reopen(t *testing.T, s *MemoryStorage) *MemoryStorage {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := NewMemoryStorage(s.walPath, 5)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	return s2
}

// A series whose samples were all flushed before a restart is still found by
// Select, because blocks carry each series' metric and labels.
func TestRestartFindsFlushedSeries(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"host": "a", "region": "apac"}
	for ts := int64(1); ts <= 5; ts++ { // the fifth sample flushes; nothing stays in the WAL
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}

	s2 := reopen(t, s)
	got := mustSelect(t, s2, 0, 100, metric("cpu"), label("region", "apac"))
	if len(got) != 1 {
		t.Fatalf("got %d series after restart, want 1", len(got))
	}
	if !reflect.DeepEqual(got[0].Labels, labels) {
		t.Errorf("labels = %v, want %v", got[0].Labels, labels)
	}
	if len(got[0].Points) != 5 {
		t.Errorf("got %d points, want 5", len(got[0].Points))
	}
}

// The ordering watermark is restored from blocks, so a restart does not let a
// sample older than already-flushed data back in.
func TestOrderingSurvivesRestart(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"host": "a"}
	for ts := int64(1); ts <= 5; ts++ {
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}

	s2 := reopen(t, s)
	older := model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 3, Value: 99}}
	if err := s2.Append(older); !errors.Is(err, compress.ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder after restart, got %v", err)
	}
	// a newer sample is still accepted
	mustAppend(t, s2, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 6, Value: 6}})
}

// After a restart, series that live only in blocks, only in the WAL, or in
// both are all selectable, and the WAL series keeps its own watermark.
func TestRestartCombinesBlocksAndWAL(t *testing.T) {
	s := newTestStore(t)
	a := model.Labels{"host": "a"}
	b := model.Labels{"host": "b"}
	for ts := int64(1); ts <= 5; ts++ { // a: flushed to a block
		mustAppend(t, s, model.Sample{Metric: "cpu", Labels: a, Point: model.Point{Timestamp: ts, Value: 1}})
	}
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: a, Point: model.Point{Timestamp: 6, Value: 1}})   // a: also in the WAL
	mustAppend(t, s, model.Sample{Metric: "cpu", Labels: b, Point: model.Point{Timestamp: 100, Value: 2}}) // b: WAL only

	s2 := reopen(t, s)
	got := mustSelect(t, s2, 0, 1000, metric("cpu"))
	if hosts := seriesHosts(got); !reflect.DeepEqual(hosts, []string{"a", "b"}) {
		t.Fatalf("got hosts %v, want [a b]", hosts)
	}
	if len(got[0].Points) != 6 {
		t.Errorf("series a: got %d points, want 6 (5 from the block, 1 from the WAL)", len(got[0].Points))
	}

	// a's watermark is 6 (from the WAL), not 5 (from the block)
	if err := s2.Append(model.Sample{Metric: "cpu", Labels: a, Point: model.Point{Timestamp: 6, Value: 9}}); !errors.Is(err, compress.ErrOutOfOrder) {
		t.Errorf("series a: expected ErrOutOfOrder for ts=6, got %v", err)
	}
	if err := s2.Append(model.Sample{Metric: "cpu", Labels: b, Point: model.Point{Timestamp: 50, Value: 9}}); !errors.Is(err, compress.ErrOutOfOrder) {
		t.Errorf("series b: expected ErrOutOfOrder for ts=50, got %v", err)
	}
}

// A block from before series identity was stored still parses as JSON, so
// without a check its series would load with no metric or labels and quietly
// never match a Select. Startup refuses it instead.
func TestOldBlockFormatIsRejected(t *testing.T) {
	dir := t.TempDir()
	blockDir := filepath.Join(dir, "blocks")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	old := `{"min_ts":1,"max_ts":5,"series":{"cpu,host=a":{"count":0,"ts":null,"val":null}}}`
	if err := os.WriteFile(filepath.Join(blockDir, "block-000001.json"), []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := NewMemoryStorage(filepath.Join(dir, "wal.log"), 5)
	if err == nil {
		t.Fatal("expected startup to fail on a block without series labels")
	}
	if !strings.Contains(err.Error(), "has no metric") {
		t.Errorf("error should explain the cause, got: %v", err)
	}
}
