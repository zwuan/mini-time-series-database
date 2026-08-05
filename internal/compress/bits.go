package compress

import "io"

type BitWriter struct {
	buf []byte
	cur byte
	nbits uint8
}

func NewBitWriter() *BitWriter {
	return &BitWriter{}
}

func (w *BitWriter) WriteBit(v uint8) {
	w.cur <<= 1
	if v & 1 == 1 {
		w.cur |= 1
	}
	w.nbits++
	if w.nbits == 8 {
		w.buf = append(w.buf, w.cur)
		w.cur = 0
		w.nbits = 0
	}
}

// Appends the lowest n bits of v, MSB-first (n = 0..64)
func (w *BitWriter) WriteBits(v uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		w.WriteBit(uint8((v >> uint(i)) & 1))
	}
}

func (w *BitWriter) Bytes() []byte {
	if w.nbits == 0 {
		return w.buf
	}

	// left-align the leftover bits into the high end of the final byte
	last := w.cur << (8 - w.nbits)
	return append(w.buf[:len(w.buf):len(w.buf)], last)
}

type BitReader struct {
	buf []byte
	nbits uint8
	pos int
}

//read individual bits, MSB-first, from a byte buffer
func NewBitReader(buf []byte) *BitReader {
	return &BitReader{buf: buf}
}

// read one bit, EOF if no more bits are available
func (r *BitReader) ReadBit() (uint8, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	b := r.buf[r.pos]
	bit := (b >> (7 - r.nbits)) & 1
	r.nbits++
	if r.nbits == 8 {
		r.nbits = 0
		r.pos++
	}
	return bit, nil
}

// read n bits, MSB-first, from the buffer, EOF if not enough bits are available
func (r *BitReader) ReadBits(n uint8) (uint64, error) {
	var v uint64
	for i := uint8(0); i < n; i++ {
		bit, err := r.ReadBit()
		if err != nil {
			return 0, err
		}
		v = (v << 1) | uint64(bit)
	}
	return v, nil
}

