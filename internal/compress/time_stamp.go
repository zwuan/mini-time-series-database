package compress

type TimestampEncoder struct {
	w *BitWriter
	prevT int64
	prevDelta int64
	n int
}

func NewTimestampEncoder() *TimestampEncoder{
	return &TimestampEncoder{ w: NewBitWriter() }
}

func (e *TimestampEncoder) Write(t int64) {
	switch e.n {
		case 0:
			// no previous point to diff against
			e.w.WriteBits(uint64(t), 64)
			e.prevT = t
		case 1:
			// first point, write delta from previous
			delta := t - e.prevT
			e.w.WriteBits(uint64(delta), 64)
			e.prevDelta = delta
			e.prevT = t
		default:
			// subsequent points, write delta of delta
			delta := t - e.prevT
			writeDoD(e.w, delta - e.prevDelta)
			e.prevDelta = delta
			e.prevT = t
		}
		e.n++
	}
	
	func (e *TimestampEncoder) Bytes() []byte {
		return e.w.Bytes()
	}

	func (e *TimestampEncoder) Count() int {
		return e.n
	}

	func writeDoD(w *BitWriter, dod int64) {
		switch {
		case dod == 0:
			w.WriteBit(0)
		case dod >= -64 && dod <= 63:
			w.WriteBits(0b10, 2)
			w.WriteBits(uint64(dod), 7)
		case dod >= -256 && dod <= 255:
			w.WriteBits(0b110, 3)
			w.WriteBits(uint64(dod), 9)
		case dod >= -2048 && dod <= 2047:
			w.WriteBits(0b1110, 4)
			w.WriteBits(uint64(dod), 12)
		default:
			w.WriteBits(0b1111, 4)
			w.WriteBits(uint64(dod), 64)
		}
	}

	// reverse TimestampEncoder
	type TimestampDecoder struct {
		r *BitReader
		prevT int64
		prevDelta int64
		n int
	}

	func NewTimestampDecoder(buf []byte) *TimestampDecoder {
		return &TimestampDecoder{ r: NewBitReader(buf) }
	}

	func (d *TimestampDecoder) Read() (int64, error) {
		switch d.n {
		case 0:
			v, err := d.r.ReadBits(64)
			if err != nil {
				return 0, err
			}
			d.prevT = int64(v)
		case 1:
			v, err := d.r.ReadBits(64)
			if err != nil {
				return 0, err
			}
			d.prevDelta = int64(v)
			d.prevT += d.prevDelta
		default:
			dod, err := readDoD(d.r)
			if err != nil {
				return 0, err
			}
			d.prevDelta += dod
			d.prevT += d.prevDelta
		}
		d.n++
		return d.prevT, nil
	}

	// count leading 1s to learn the playload width (up to 4)
	func readDoD(r *BitReader) (int64, error){
		ones := 0
		for ones < 4 {
			bit, err := r.ReadBit()
			if err != nil {
				return 0, err
			}
			if bit == 0 {
				break
			}
			ones++
		}

		var nbits uint8
		switch ones {
		case 0:
			return 0, nil
		case 1:
			nbits = 7
		case 2:
			nbits = 9
		case 3:
			nbits = 12
		default:
			nbits = 64
		}
		
		v, err := r.ReadBits(nbits)
		if err != nil {
			return 0, err
		}
		return signExtend(v, nbits), nil
	}
 
	func signExtend(v uint64, n uint8) int64 {
		if n < 64 && v&(1<<(n-1)) != 0 {
			v |= ^uint64(0) << n
		}
		return int64(v)
	}