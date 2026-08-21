package compress

import (
	"math"
	"testing"
)

func roundTrip(t *testing.T, in []int64) []byte {
	t.Helper()
	e := NewTimestampEncoder()
	for _, ts := range in {
		e.Write(ts)
	}
	buf := e.Bytes()

	d := NewTimestampDecoder(buf)
	for i, want := range in {
		got, err := d.Read()
		if err != nil {
			t.Fatalf("Read at index %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("index %d: got %d, want %d", i, got, want)
		}
	}
	return buf
}

// A regular scrape interval is the case Gorilla optimises for.
func TestTimestampRegularInterval(t *testing.T) {
	in := []int64{1600000000, 1600000010, 1600000020, 1600000030, 1600000040}
	buf := roundTrip(t, in)
	t.Logf("%d timestamps -> %d bytes (raw would be %d)", len(in), len(buf), len(in)*8)
}

// Every control-bit branch, including the sign-extension edges of each width.
func TestTimestampAllControlBranches(t *testing.T) {
	deltas := []int64{
		10,        // first delta
		10,        // dod 0     -> '0'
		10 - 64,   // dod -64   -> '10'   lower edge
		10 + 63,   // dod +63   -> '10'   upper edge
		10 - 256,  // dod -256  -> '110'  lower edge
		10 + 255,  // dod +255  -> '110'  upper edge
		10 - 2048, // dod -2048 -> '1110' lower edge
		10 + 2047, // dod +2047 -> '1110' upper edge
		100000,    // dod large -> '1111'
		-100000,   // dod large negative
	}
	in := []int64{1000}
	cur := int64(1000)
	for _, d := range deltas {
		cur += d
		in = append(in, cur)
	}
	roundTrip(t, in)
}

func TestTimestampEdgeCases(t *testing.T) {
	roundTrip(t, []int64{5})                                                      // single point
	roundTrip(t, []int64{0, 0, 0, 0})                                             // all identical
	roundTrip(t, []int64{-1000, -900, -800, -700})                                // negative
	roundTrip(t, []int64{500, 400, 300, 200})                                     // decreasing
	roundTrip(t, []int64{math.MaxInt64 - 100, math.MaxInt64 - 50, math.MaxInt64}) // extremes
}

func TestTimestampIrregularInterval(t *testing.T) {
	in := []int64{1000}
	cur := int64(1000)
	for _, s := range []int64{60, 60, 61, 59, 60, 3600, 60, 60, 1, 60} {
		cur += s
		in = append(in, cur)
	}
	roundTrip(t, in)
}

// The headline number: how close do we get to ~1 bit per timestamp?
func TestTimestampCompressionRatio(t *testing.T) {
	in := make([]int64, 1000)
	ts := int64(1600000000)
	for i := range in {
		in[i] = ts
		ts += 10
	}
	buf := roundTrip(t, in)

	raw := len(in) * 8
	ratio := float64(raw) / float64(len(buf))
	bitsPerPoint := float64(len(buf)*8) / float64(len(in))
	t.Logf("1000 regular timestamps: %d bytes vs %d raw = %.1fx (%.2f bits/point)",
		len(buf), raw, ratio, bitsPerPoint)

	if bitsPerPoint > 2 {
		t.Errorf("expected under 2 bits per timestamp, got %.2f", bitsPerPoint)
	}
}
