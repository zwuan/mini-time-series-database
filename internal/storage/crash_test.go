package storage

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"minitsdb/internal/model"
)

// interruptFlush recreates what the WAL holds when the process stops after a
// flush has renamed its block into place but before it has truncated the WAL:
// the flushed samples are in the block and still in the log.
func interruptFlush(t *testing.T, s *MemoryStorage, samples []model.Sample) {
	t.Helper()
	var wal []byte
	for _, sm := range samples {
		mustAppend(t, s, sm)
		line, err := json.Marshal(sm)
		if err != nil {
			t.Fatal(err)
		}
		wal = append(wal, append(line, '\n')...)
	}
	if s.BlockSeq == 0 {
		t.Fatal("setup: the samples should have triggered a flush")
	}
	if err := os.WriteFile(s.walPath, wal, 0o644); err != nil {
		t.Fatal(err)
	}
}

// Regression: replay used to append every WAL sample, so a flush interrupted
// before its WAL truncate left each flushed sample stored twice.
func TestReplaySkipsSamplesAlreadyInBlocks(t *testing.T) {
	s := newTestStore(t) // flushes every 5 samples
	a := model.Labels{"host": "a"}
	b := model.Labels{"host": "b"}
	var samples []model.Sample
	for ts := int64(1); ts <= 3; ts++ {
		samples = append(samples, model.Sample{Metric: "cpu", Labels: a, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}
	for ts := int64(1); ts <= 2; ts++ {
		samples = append(samples, model.Sample{Metric: "cpu", Labels: b, Point: model.Point{Timestamp: ts, Value: float64(ts) * 10}})
	}
	interruptFlush(t, s, samples)

	s2 := reopen(t, s)
	wantA := []model.Point{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}}
	if got := mustQuery(t, s2, "cpu", a, 0, 100); !reflect.DeepEqual(got, wantA) {
		t.Errorf("series a: got %v, want %v", got, wantA)
	}
	wantB := []model.Point{{Timestamp: 1, Value: 10}, {Timestamp: 2, Value: 20}}
	if got := mustQuery(t, s2, "cpu", b, 0, 100); !reflect.DeepEqual(got, wantB) {
		t.Errorf("series b: got %v, want %v", got, wantB)
	}
}

// Skipping stops at the watermark: a WAL sample newer than everything in the
// blocks is still restored.
func TestReplayKeepsSamplesNewerThanBlocks(t *testing.T) {
	s := newTestStore(t)
	labels := model.Labels{"host": "a"}
	var samples []model.Sample
	for ts := int64(1); ts <= 5; ts++ {
		samples = append(samples, model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: ts, Value: float64(ts)}})
	}
	interruptFlush(t, s, samples)

	newer, err := json.Marshal(model.Sample{Metric: "cpu", Labels: labels, Point: model.Point{Timestamp: 6, Value: 6}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(s.walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(append(newer, '\n'))
	f.Close()

	s2 := reopen(t, s)
	got := mustQuery(t, s2, "cpu", labels, 0, 100)
	if len(got) != 6 || got[5].Timestamp != 6 {
		t.Fatalf("got %v, want 1..6 with no duplicates", got)
	}
}
