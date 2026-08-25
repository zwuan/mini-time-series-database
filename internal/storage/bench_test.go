package storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"minitsdb/internal/compress"
	"minitsdb/internal/model"
)

// blockDirSize reports the total bytes of every block file under dir.
func blockDirSize(t testing.TB, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var total int64
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		total += fi.Size()
	}
	return total
}

// TestEndToEndCompression measures what actually lands on disk, including the
// JSON and base64 overhead of the block format -- not just the encoders.
func TestEndToEndCompression(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement test; run without -short")
	}
	const n = 10000
	shapes := []struct {
		name string
		gen  func(i int) float64
	}{
		{"constant value", func(i int) float64 { return 42.0 }},
		{"integer gauge", func(i int) float64 { return float64(40 + i%7) }},
		{"monotonic counter", func(i int) float64 { return float64(i * 3) }},
		{"accumulating float", func(i int) float64 { return 23.5 + float64(i)*0.1 }},
		{"random float", func(i int) float64 { return rand.Float64() * 1e6 }},
	}

	for _, sh := range shapes {
		dir := t.TempDir()
		s, err := NewMemoryStorage(filepath.Join(dir, "wal.log"), 1000)
		if err != nil {
			t.Fatalf("create store: %v", err)
		}
		labels := model.Labels{"host": "a", "region": "apac"}
		ts := int64(1600000000)
		for i := 0; i < n; i++ {
			if err := s.Append(model.Sample{
				Metric: "cpu", Labels: labels,
				Point: model.Point{Timestamp: ts, Value: sh.gen(i)},
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
			ts += 10
		}
		if err := s.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		onDisk := blockDirSize(t, filepath.Join(dir, "blocks"))
		raw := int64(n * 16) // int64 timestamp + float64 value
		t.Logf("%-20s %7d B on disk vs %7d B raw = %5.1fx (%.1f bits/sample)",
			sh.name, onDisk, raw, float64(raw)/float64(onDisk),
			float64(onDisk*8)/float64(n))
	}
}

// TestFlushThresholdTradeoff shows how write throughput depends on how often
// the head is flushed. Each flush costs three fsyncs, so its cost is roughly
// constant and amortises over the samples buffered before it.
func TestFlushThresholdTradeoff(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement test; run without -short")
	}
	const n = 20000
	for _, threshold := range []int{100, 1000, 10000, 1 << 30} {
		dir := t.TempDir()
		s, err := NewMemoryStorage(filepath.Join(dir, "wal.log"), threshold)
		if err != nil {
			t.Fatalf("create store: %v", err)
		}
		labels := model.Labels{"host": "a"}
		ts := int64(1600000000)

		start := time.Now()
		for i := 0; i < n; i++ {
			if err := s.Append(model.Sample{
				Metric: "cpu", Labels: labels,
				Point: model.Point{Timestamp: ts, Value: float64(i % 100)},
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
			ts += 10
		}
		elapsed := time.Since(start)
		s.Close()

		label := "never flushed"
		if threshold < 1<<30 {
			label = fmt.Sprintf("flush every %d", threshold)
		}
		t.Logf("%-22s %8.0f points/sec (%6.2f us/point)",
			label, float64(n)/elapsed.Seconds(),
			float64(elapsed.Microseconds())/float64(n))
	}
}

func BenchmarkAppend(b *testing.B) {
	s, err := NewMemoryStorage(filepath.Join(b.TempDir(), "wal.log"), DefaultFlushThreshold)
	if err != nil {
		b.Fatalf("create store: %v", err)
	}
	defer s.Close()

	labels := model.Labels{"host": "a"}
	ts := int64(1600000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Append(model.Sample{
			Metric: "cpu", Labels: labels,
			Point: model.Point{Timestamp: ts, Value: float64(i % 100)},
		}); err != nil {
			b.Fatalf("append: %v", err)
		}
		ts += 10
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "points/sec")
}

// BenchmarkAppendParts breaks down the cost inside Append, to show where the
// time actually goes: compression is a rounding error next to the WAL write.
func BenchmarkAppendParts(b *testing.B) {
	labels := model.Labels{"host": "a"}
	sample := model.Sample{Metric: "cpu", Labels: labels,
		Point: model.Point{Timestamp: 1600000000, Value: 42}}

	b.Run("SeriesKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			model.SeriesKey("cpu", labels)
		}
	})
	b.Run("ChunkAppend", func(b *testing.B) {
		c := compress.NewChunk()
		ts := int64(1600000000)
		for i := 0; i < b.N; i++ {
			if err := c.Append(ts, float64(i%100)); err != nil {
				b.Fatal(err)
			}
			ts += 10
		}
	})
	b.Run("JSONMarshal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(sample); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("WALWrite", func(b *testing.B) {
		f, err := os.OpenFile(filepath.Join(b.TempDir(), "w.log"),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			b.Fatal(err)
		}
		defer f.Close()
		line, _ := json.Marshal(sample)
		line = append(line, '\n')
		for i := 0; i < b.N; i++ {
			if _, err := f.Write(line); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkQueryHead decodes 10k samples out of the in-memory chunk.
func BenchmarkQueryHead(b *testing.B) {
	s, err := NewMemoryStorage(filepath.Join(b.TempDir(), "wal.log"), 1<<30)
	if err != nil {
		b.Fatalf("create store: %v", err)
	}
	defer s.Close()
	labels := seedSamples(b, s, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := s.Query("cpu", labels, 0, 1<<62)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		if len(got) != 10000 {
			b.Fatalf("got %d points", len(got))
		}
	}
}

// BenchmarkQueryBlocks reads and decodes the same 10k samples from disk.
func BenchmarkQueryBlocks(b *testing.B) {
	dir := b.TempDir()
	s, err := NewMemoryStorage(filepath.Join(dir, "wal.log"), 1000)
	if err != nil {
		b.Fatalf("create store: %v", err)
	}
	labels := seedSamples(b, s, 10000)
	s.Close()

	s2, err := NewMemoryStorage(filepath.Join(dir, "wal.log"), 1000)
	if err != nil {
		b.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := s2.Query("cpu", labels, 0, 1<<62)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		if len(got) != 10000 {
			b.Fatalf("got %d points", len(got))
		}
	}
}

func seedSamples(b *testing.B, s *MemoryStorage, n int) model.Labels {
	b.Helper()
	labels := model.Labels{"host": "a"}
	ts := int64(1600000000)
	for i := 0; i < n; i++ {
		if err := s.Append(model.Sample{
			Metric: "cpu", Labels: labels,
			Point: model.Point{Timestamp: ts, Value: float64(i % 100)},
		}); err != nil {
			b.Fatalf("seed append: %v", err)
		}
		ts += 10
	}
	return labels
}

// TestSyncOnWriteCost measures the price of fsyncing the WAL on every append.
// Without it, samples written since the last flush live only in the OS page
// cache and are lost on power failure.
//
// The assertion guards the option actually taking effect: forgetting to apply
// the functional options in the constructor leaves syncOnWrite false, which
// compiles and produces no visible failure other than a suspiciously fast run.
func TestSyncOnWriteCost(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement test; run without -short")
	}
	const n = 2000
	rates := map[bool]float64{}

	for _, sync := range []bool{false, true} {
		dir := t.TempDir()
		var opts []Option
		if sync {
			opts = append(opts, WithSyncOnWrite())
		}
		s, err := NewMemoryStorage(filepath.Join(dir, "wal.log"), 1<<30, opts...)
		if err != nil {
			t.Fatalf("create store: %v", err)
		}
		labels := model.Labels{"host": "a"}
		ts := int64(1600000000)

		start := time.Now()
		for i := 0; i < n; i++ {
			if err := s.Append(model.Sample{
				Metric: "cpu", Labels: labels,
				Point: model.Point{Timestamp: ts, Value: float64(i % 100)},
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
			ts += 10
		}
		elapsed := time.Since(start)
		s.Close()

		rates[sync] = float64(n) / elapsed.Seconds()
		label := "WAL buffered (default)"
		if sync {
			label = "fsync every append"
		}
		t.Logf("%-24s %8.0f points/sec (%7.1f us/point)",
			label, rates[sync], float64(elapsed.Microseconds())/float64(n))
	}

	// An fsync waits on the physical device, so it must cost far more than a
	// buffered write. Anything close to parity means no fsync happened.
	if rates[true] > rates[false]/10 {
		t.Errorf("fsync should be at least 10x slower, but got %.0f points/sec synced vs %.0f buffered -- is WithSyncOnWrite() being applied?",
			rates[true], rates[false])
	}
}
