package compress

import (
	"errors"
	"math"
	"math/rand"
	"testing"
)

func TestChunkRoundTrip(t *testing.T) {
	c := NewChunk()
	ts := int64(1600000000)
	for i := 0; i < 100; i++ {
		if err := c.Append(ts, 23.5); err != nil {
			t.Fatalf("append: %v", err)
		}
		ts += 10
	}

	it := c.Iterator()
	want := int64(1600000000)
	n := 0
	for it.Next() {
		gotT, gotV := it.At()
		if gotT != want || gotV != 23.5 {
			t.Fatalf("sample %d: got (%d, %v), want (%d, 23.5)", n, gotT, gotV, want)
		}
		want += 10
		n++
	}
	if it.Err() != nil {
		t.Fatalf("iterator error: %v", it.Err())
	}
	if n != 100 {
		t.Fatalf("decoded %d samples, want 100", n)
	}
}

// A rejected sample must not be stored, and must not corrupt the stream.
func TestChunkRejectsOutOfOrder(t *testing.T) {
	c := NewChunk()
	if err := c.Append(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.Append(200, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.Append(150, 3); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("older timestamp: expected ErrOutOfOrder, got %v", err)
	}
	if err := c.Append(200, 4); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("duplicate timestamp: expected ErrOutOfOrder, got %v", err)
	}
	if c.Count() != 2 {
		t.Fatalf("rejected samples must not be counted, got %d", c.Count())
	}

	it := c.Iterator()
	var got []int64
	for it.Next() {
		ts, _ := it.At()
		got = append(got, ts)
	}
	if len(got) != 2 || got[0] != 100 || got[1] != 200 {
		t.Fatalf("stream corrupted after rejection: %v", got)
	}
}

// The head block is queried while it is still being written to.
func TestChunkIterateWhileAppending(t *testing.T) {
	c := NewChunk()
	for i := int64(1); i <= 5; i++ {
		if err := c.Append(i*10, float64(i)); err != nil {
			t.Fatal(err)
		}
	}

	it := c.Iterator()
	n := 0
	for it.Next() {
		n++
	}
	if n != 5 {
		t.Fatalf("snapshot has %d samples, want 5", n)
	}

	// appending after a snapshot must still work
	for i := int64(6); i <= 10; i++ {
		if err := c.Append(i*10, float64(i)); err != nil {
			t.Fatalf("append after snapshot: %v", err)
		}
	}

	it2 := c.Iterator()
	n = 0
	var last int64
	for it2.Next() {
		last, _ = it2.At()
		n++
	}
	if n != 10 || last != 100 {
		t.Fatalf("after appending: n=%d last=%d, want 10 and 100", n, last)
	}
}

func TestChunkEmpty(t *testing.T) {
	c := NewChunk()
	if c.Count() != 0 {
		t.Fatalf("empty chunk count = %d", c.Count())
	}
	if c.Iterator().Next() {
		t.Fatal("empty chunk should not yield samples")
	}
}

// Simulates a chunk written to a block file and read back.
func TestChunkFromDisk(t *testing.T) {
	c := NewChunk()
	for i := int64(0); i < 50; i++ {
		if err := c.Append(1000+i*15, float64(i)*1.5); err != nil {
			t.Fatal(err)
		}
	}
	tsBuf, valBuf := c.Bytes()

	it := NewChunkIterator(tsBuf, valBuf, c.Count())
	i := int64(0)
	for it.Next() {
		gotT, gotV := it.At()
		if gotT != 1000+i*15 || gotV != float64(i)*1.5 {
			t.Fatalf("sample %d: got (%d, %v)", i, gotT, gotV)
		}
		i++
	}
	if i != 50 {
		t.Fatalf("decoded %d samples, want 50", i)
	}
}

func TestChunkFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	type sample struct {
		t int64
		v float64
	}
	for round := 0; round < 100; round++ {
		c := NewChunk()
		n := r.Intn(200) + 1
		ts := int64(r.Intn(1000))
		want := make([]sample, 0, n)

		for i := 0; i < n; i++ {
			ts += int64(r.Intn(120) + 1) // strictly increasing
			var v float64
			switch r.Intn(3) {
			case 0:
				v = float64(r.Intn(100))
			case 1:
				v = r.Float64() * 1e6
			default:
				v = math.Float64frombits(r.Uint64())
			}
			if err := c.Append(ts, v); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			want = append(want, sample{ts, v})
		}

		it := c.Iterator()
		i := 0
		for it.Next() {
			gotT, gotV := it.At()
			if gotT != want[i].t {
				t.Fatalf("round %d sample %d: ts %d, want %d", round, i, gotT, want[i].t)
			}
			if gotV != want[i].v && !(math.IsNaN(gotV) && math.IsNaN(want[i].v)) {
				t.Fatalf("round %d sample %d: val %v, want %v", round, i, gotV, want[i].v)
			}
			i++
		}
		if i != n {
			t.Fatalf("round %d: decoded %d, want %d", round, i, n)
		}
	}
}

// The headline number: timestamps and values together.
func TestChunkCompressionRatio(t *testing.T) {
	cases := []struct {
		name string
		gen  func(i int) (int64, float64)
	}{
		{"constant value", func(i int) (int64, float64) {
			return int64(1600000000 + i*10), 42.0
		}},
		{"integer gauge", func(i int) (int64, float64) {
			return int64(1600000000 + i*10), float64(40 + i%7)
		}},
		{"accumulating float", func(i int) (int64, float64) {
			return int64(1600000000 + i*10), 23.5 + float64(i)*0.1
		}},
	}

	const n = 1000
	for _, tc := range cases {
		c := NewChunk()
		for i := 0; i < n; i++ {
			ts, v := tc.gen(i)
			if err := c.Append(ts, v); err != nil {
				t.Fatal(err)
			}
		}
		raw := n * 16 // int64 timestamp + float64 value
		t.Logf("%-20s %5d B vs %5d raw = %5.1fx (%.1f bits/sample)",
			tc.name, c.Size(), raw, float64(raw)/float64(c.Size()),
			float64(c.Size()*8)/float64(n))
	}
}
