package compress

import (
	"fmt"
)

var ErrOutOfOrder = fmt.Errorf("compress: sample is out of order")

type Chunk struct {
	ts    *TimestampEncoder
	val   *ValueEncoder
	minTS int64
	maxTS int64
	n     int
}

func NewChunk() *Chunk {
	return &Chunk{
		ts:    NewTimestampEncoder(),
		val:   NewValueEncoder(),
		minTS: 0,
		maxTS: 0,
		n:     0,
	}
}

func (c *Chunk) Append(t int64, v float64) error {
	// Check for out-of-order timestamps
	if c.n > 0 && t <= c.maxTS {
		return fmt.Errorf("%w: got %d, last was %d", ErrOutOfOrder, t, c.maxTS)
	}
	if c.n == 0 {
		c.minTS = t
	}
	c.ts.Write(t)
	c.val.Write(v)
	c.maxTS = t
	c.n++
	return nil
}

func (c *Chunk) Count() int   { return c.n }
func (c *Chunk) MinTS() int64 { return c.minTS }
func (c *Chunk) MaxTS() int64 { return c.maxTS }

// Bytes returns the underlying bytes of the chunk, which consists of the timestamp bytes followed by the value bytes
func (c *Chunk) Bytes() (ts []byte, val []byte) {
	return c.ts.Bytes(), c.val.Bytes()
}

func (c *Chunk) Size() int {
	t, v := c.Bytes()
	return len(t) + len(v)
}

func (c *Chunk) Iterator() *ChunkIterator {
	tsBuf, valBuf := c.Bytes()
	return NewChunkIterator(tsBuf, valBuf, c.n)
}

// NewChunkIterator creates a new ChunkIterator from the given timestamp and value buffers, along with the count of samples
func NewChunkIterator(tsBuf, valBuf []byte, count int) *ChunkIterator {
	return &ChunkIterator{
		ts:  NewTimestampDecoder(tsBuf),
		val: NewValueDecoder(valBuf),
		n:   count,
	}
}

type ChunkIterator struct {
	ts   *TimestampDecoder
	val  *ValueDecoder
	n    int
	i    int
	curT int64
	curV float64
	err  error
}

func (it *ChunkIterator) Next() bool {
	if it.i >= it.n || it.err != nil {
		return false
	}
	t, err := it.ts.Read()
	if err != nil {
		it.err = err
		return false
	}
	v, err := it.val.Read()
	if err != nil {
		it.err = err
		return false
	}
	it.curT, it.curV = t, v
	it.i++
	return true
}

func (it *ChunkIterator) At() (int64, float64) {
	return it.curT, it.curV
}

func (it *ChunkIterator) Err() error {
	return it.err
}
