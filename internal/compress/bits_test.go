package compress

import (
	"io"
	"math"
	"testing"
)

// Single bits and multi-bit values must survive a write/read round trip,
// including values that straddle byte boundaries.
func TestBitRoundTrip(t *testing.T) {
	w := NewBitWriter()
	w.WriteBit(1)
	w.WriteBit(0)
	w.WriteBit(1)
	w.WriteBits(0b1101, 4)
	w.WriteBits(0x2A, 8)  // crosses a byte boundary
	w.WriteBits(0x1FF, 9) // needs more than 8 bits

	r := NewBitReader(w.Bytes())

	expectBit := func(want uint8) {
		t.Helper()
		got, err := r.ReadBit()
		if err != nil {
			t.Fatalf("ReadBit: %v", err)
		}
		if got != want {
			t.Fatalf("bit: got %d, want %d", got, want)
		}
	}
	expectBits := func(want uint64, n uint8) {
		t.Helper()
		got, err := r.ReadBits(n)
		if err != nil {
			t.Fatalf("ReadBits(%d): %v", n, err)
		}
		if got != want {
			t.Fatalf("bits(%d): got %d, want %d", n, got, want)
		}
	}

	expectBit(1)
	expectBit(0)
	expectBit(1)
	expectBits(0b1101, 4)
	expectBits(0x2A, 8)
	expectBits(0x1FF, 9)
}

// The encodings in the next steps need the full 64-bit width: raw timestamps
// and float64 bit patterns.
func TestWriteBits64BitWidth(t *testing.T) {
	values := []uint64{
		0,
		1,
		math.MaxUint64,
		math.Float64bits(3.14159),
		1 << 63,
		1600000000, // a realistic unix timestamp
	}

	w := NewBitWriter()
	for _, v := range values {
		w.WriteBits(v, 64)
	}

	r := NewBitReader(w.Bytes())
	for i, want := range values {
		got, err := r.ReadBits(64)
		if err != nil {
			t.Fatalf("value %d: ReadBits: %v", i, err)
		}
		if got != want {
			t.Errorf("value %d: got %d, want %d", i, got, want)
		}
	}
}

// A bit count that is not a multiple of 8 leaves a partial final byte;
// Bytes() must pad it so the reader still sees the bits in order.
func TestPartialFinalByte(t *testing.T) {
	w := NewBitWriter()
	// 11 bits total -> 2 bytes, 5 padding bits
	w.WriteBits(0b101, 3)
	w.WriteBits(0b11110000, 8)

	buf := w.Bytes()
	if len(buf) != 2 {
		t.Fatalf("expected 2 bytes for 11 bits, got %d", len(buf))
	}

	r := NewBitReader(buf)
	got, err := r.ReadBits(3)
	if err != nil {
		t.Fatalf("ReadBits(3): %v", err)
	}
	if got != 0b101 {
		t.Errorf("first 3 bits: got %03b, want 101", got)
	}
	got, err = r.ReadBits(8)
	if err != nil {
		t.Fatalf("ReadBits(8): %v", err)
	}
	if got != 0b11110000 {
		t.Errorf("next 8 bits: got %08b, want 11110000", got)
	}
}

// An exact multiple of 8 must not append a spurious padding byte.
func TestNoPaddingWhenByteAligned(t *testing.T) {
	w := NewBitWriter()
	w.WriteBits(0xAB, 8)
	w.WriteBits(0xCD, 8)

	buf := w.Bytes()
	if len(buf) != 2 {
		t.Fatalf("expected exactly 2 bytes, got %d", len(buf))
	}
	if buf[0] != 0xAB || buf[1] != 0xCD {
		t.Errorf("got % X, want AB CD", buf)
	}
}

// Reading past the end must report io.EOF rather than returning garbage.
func TestReadPastEndReturnsEOF(t *testing.T) {
	w := NewBitWriter()
	w.WriteBits(0xFF, 8)

	r := NewBitReader(w.Bytes())
	if _, err := r.ReadBits(8); err != nil {
		t.Fatalf("first 8 bits should succeed: %v", err)
	}
	if _, err := r.ReadBit(); err != io.EOF {
		t.Errorf("expected io.EOF past the end, got %v", err)
	}
}

// Bytes() is called once writing is done; calling it must not corrupt the
// buffer for a subsequent call.
func TestBytesIsRepeatable(t *testing.T) {
	w := NewBitWriter()
	w.WriteBits(0b1010101, 7) // partial byte

	first := append([]byte(nil), w.Bytes()...)
	second := w.Bytes()

	if len(first) != len(second) {
		t.Fatalf("length changed: %d then %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("byte %d differs: % X vs % X", i, first, second)
		}
	}
}
