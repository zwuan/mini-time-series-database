package compress

import (
	"math"
	mbits "math/bits"
)

const noWindow = 0xFF

type ValueEncoder struct {
	w        *BitWriter
	prev     uint64
	leading  uint8
	trailing uint8
	n        int
}

func NewValueEncoder() *ValueEncoder {
	return &ValueEncoder{w: NewBitWriter(), leading: noWindow}
}

// Write encodes a float64 value to the underlying bit stream
func (e *ValueEncoder) Write(v float64) {
	cur := math.Float64bits(v)
	if e.n == 0 {
		e.w.WriteBits(cur, 64)
		e.prev = cur
		e.n++
		return
	}

	xor := e.prev ^ cur
	if xor == 0 {
		e.w.WriteBit(0)
	} else {
		e.w.WriteBit(1)

		lead := uint8(mbits.LeadingZeros64(xor))
		trail := uint8(mbits.TrailingZeros64(xor))
		if lead >= 32 {
			lead = 31
		}

		// Values whose XOR keeps the same shape should reuse the window
		if e.leading != noWindow && lead >= e.leading && trail >= e.trailing {
			e.w.WriteBit(0)
			sig := 64 - e.leading - e.trailing
			e.w.WriteBits(xor>>e.trailing, sig)
		} else {
			e.w.WriteBit(1)
			sig := 64 - lead - trail
			e.w.WriteBits(uint64(lead), 5)
			e.w.WriteBits(uint64(sig&0x3F), 6)
			e.w.WriteBits(xor>>trail, sig)
			e.leading = lead
			e.trailing = trail
		}
	}
	e.prev = cur
	e.n++
}

func (e *ValueEncoder) Bytes() []byte {
	return e.w.Bytes()
}

func (e *ValueEncoder) Count() int {
	return e.n
}

type ValueDecoder struct {
	r        *BitReader
	prev     uint64
	leading  uint8
	trailing uint8
	n        int
}

func NewValueDecoder(buf []byte) *ValueDecoder {
	return &ValueDecoder{r: NewBitReader(buf), leading: noWindow}
}

func (d *ValueDecoder) Read() (float64, error) {
	if d.n == 0 {
		v, err := d.r.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.prev = v
		d.n++
		return math.Float64frombits(v), nil
	}

	bit, err := d.r.ReadBit()
	if err != nil {
		return 0, err
	}

	// if the bit is 0, the value is the same as the previous one
	if bit == 1 {
		ctrl, err := d.r.ReadBit()
		if err != nil {
			return 0, err
		}
		if ctrl == 1 {
			l, err := d.r.ReadBits(5)
			if err != nil {
				return 0, err
			}
			s, err := d.r.ReadBits(6)
			if err != nil {
				return 0, err
			}
			sig := uint8(s)
			if sig == 0 {
				sig = 64
			}
			d.leading = uint8(l)
			d.trailing = 64 - d.leading - sig
		}

		sig := 64 - d.leading - d.trailing
		v, err := d.r.ReadBits(sig)
		if err != nil {
			return 0, err
		}
		// XOR its own inverse
		d.prev ^= v << d.trailing
	}
	d.n++
	return math.Float64frombits(d.prev), nil
}
