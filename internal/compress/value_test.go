package compress

import (
	"math"
	"math/rand"
	"testing"
)

func valueRoundTrip(t *testing.T, in []float64) []byte {
	t.Helper()
	e := NewValueEncoder()
	for _, v := range in {
		e.Write(v)
	}
	buf := e.Bytes()

	d := NewValueDecoder(buf)
	for i, want := range in {
		got, err := d.Read()
		if err != nil {
			t.Fatalf("Read at index %d: %v", i, err)
		}
		if math.IsNaN(want) {
			if !math.IsNaN(got) {
				t.Fatalf("index %d: got %v, want NaN", i, got)
			}
			continue
		}
		if got != want {
			t.Fatalf("index %d: got %v (%016x), want %v (%016x)",
				i, got, math.Float64bits(got), want, math.Float64bits(want))
		}
	}
	return buf
}

// An unchanging series is the best case: one bit per value.
func TestValueConstant(t *testing.T) {
	in := make([]float64, 100)
	for i := range in {
		in[i] = 42.0
	}
	buf := valueRoundTrip(t, in)
	t.Logf("100 identical values -> %d bytes (raw 800), %.1f bits/value",
		len(buf), float64(len(buf)*8)/float64(len(in)))
}

func TestValueEdgeCases(t *testing.T) {
	valueRoundTrip(t, []float64{0})
	valueRoundTrip(t, []float64{0, 0, 0})
	valueRoundTrip(t, []float64{0, 1, 0, 1, 0})
	valueRoundTrip(t, []float64{-0.0, 0.0, -0.0})      // signed zero
	valueRoundTrip(t, []float64{5.5, -5.5, 5.5, -5.5}) // sign flips: xor hits the top bit
	valueRoundTrip(t, []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, 0})
	valueRoundTrip(t, []float64{math.Inf(1), math.Inf(-1), 0, math.NaN(), 1})
	valueRoundTrip(t, []float64{1e-300, 1e300, -1e300})                  // denormals and extremes
	valueRoundTrip(t, []float64{1.0000000000000002, 1.0000000000000004}) // low-bit changes only
}

// Values whose XOR keeps the same shape should reuse the window.
func TestValueWindowReuse(t *testing.T) {
	valueRoundTrip(t, []float64{100.0, 100.5, 101.0, 101.5, 102.0, 102.5})
}

// Random bit patterns are the adversarial case: nothing should ever fail to
// round-trip, however badly it compresses.
func TestValueFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for round := 0; round < 200; round++ {
		in := make([]float64, r.Intn(50)+1)
		for i := range in {
			switch r.Intn(4) {
			case 0:
				in[i] = r.Float64()
			case 1:
				in[i] = r.Float64() * 1e9
			case 2:
				in[i] = float64(r.Intn(10))
			default:
				in[i] = math.Float64frombits(r.Uint64())
			}
		}
		valueRoundTrip(t, in)
	}
}
