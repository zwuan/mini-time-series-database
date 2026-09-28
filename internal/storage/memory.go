package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"minitsdb/internal/compress"
	"minitsdb/internal/index"
	"minitsdb/internal/model"
)

const blockFilePrefix = "block-"
const blockFileSuffix = ".json"

// seriesMeta is a series' identity. It outlives the head: a flush moves the
// samples to disk, but the series keeps its ID, its index entries, and the
// timestamp that later samples must exceed.
type seriesMeta struct {
	key    string
	metric string
	labels model.Labels
	lastTS int64
}

type MemoryStorage struct {
	mu     sync.RWMutex
	series map[string]*compress.Chunk // head: samples not yet flushed
	wal    *os.File

	ids   map[string]index.SeriesID // series key -> ID
	meta  []seriesMeta              // indexed by SeriesID
	index *index.Index

	walPath        string
	blockDir       string
	BlockSeq       int
	pointCount     int
	flushThreshold int
	syncOnWrite    bool
}

// Option is a functional option for configuring the MemoryStorage
type Option func(*MemoryStorage)

func WithSyncOnWrite() Option {
	return func(s *MemoryStorage) { s.syncOnWrite = true }
}

type blockSeries struct {
	Count int    `json:"count"`
	TS    []byte `json:"ts"`
	Val   []byte `json:"val"`
}

// block is the immutable on-disk block file format.
// min_ts / max_ts let queries skip irrelevant blocks (block pruning).
type block struct {
	MinTS  int64                  `json:"min_ts"`
	MaxTS  int64                  `json:"max_ts"`
	Series map[string]blockSeries `json:"series"`
}

const DefaultFlushThreshold = 1000

func NewMemoryStorage(walPath string, flushThreshold int, opts ...Option) (*MemoryStorage, error) {
	if flushThreshold <= 0 {
		flushThreshold = DefaultFlushThreshold
	}

	if err := os.MkdirAll(filepath.Dir(walPath), 0o755); err != nil {
		return nil, err
	}

	// make block directory
	blockDir := filepath.Join(filepath.Dir(walPath), "blocks")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	s := &MemoryStorage{
		series:         make(map[string]*compress.Chunk),
		wal:            f,
		ids:            make(map[string]index.SeriesID),
		index:          index.New(),
		walPath:        walPath,
		blockDir:       blockDir,
		BlockSeq:       0,
		pointCount:     0,
		flushThreshold: flushThreshold,
	}

	for _, opt := range opts {
		opt(s)
	}

	// Continue numbering from existing blocks so a restart never overwrites old ones.
	seq, err := scanMaxBlockSeq(blockDir)
	if err != nil {
		f.Close()
		return nil, err
	}
	s.BlockSeq = seq

	// Blocks stay on disk and are not loaded back; the WAL only holds samples written since the last checkpoint.
	if err := s.replay(walPath); err != nil {
		f.Close()
		return nil, err
	}

	return s, nil
}

// scanMaxBlockSeq returns the highest existing block sequence number in blockDir (0 if none).
func scanMaxBlockSeq(blockDir string) (int, error) {
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		return 0, err
	}
	max := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, blockFilePrefix) || !strings.HasSuffix(name, blockFileSuffix) {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(name, blockFilePrefix), blockFileSuffix)
		n, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	return max, nil
}

func (s *MemoryStorage) replay(walPath string) error {
	f, err := os.Open(walPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var sample model.Sample
		if err := json.Unmarshal(line, &sample); err != nil {
			return err
		}
		if err := s.appendMemory(sample); err != nil {
			return fmt.Errorf("replay sample %d: %w", count+1, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	log.Printf("WAL replay done: restored %d samples", count)
	return nil
}

// appendMemory writes the sample into the series' compressed chunk, creating the chunk on first use.
func (s *MemoryStorage) appendMemory(sample model.Sample) error {
	key := model.SeriesKey(sample.Metric, sample.Labels)
	c := s.series[key]
	if c == nil {
		c = compress.NewChunk()
		s.series[key] = c
	}
	if err := c.Append(sample.Point.Timestamp, sample.Point.Value); err != nil {
		return err
	}
	id := s.register(key, sample)
	s.meta[id].lastTS = sample.Point.Timestamp
	s.pointCount++
	return nil
}

// register returns the series' ID, assigning one and adding the series to the
// index the first time it is seen. IDs are handed out in increasing order, so
// every posting list is appended to rather than inserted into.
func (s *MemoryStorage) register(key string, sample model.Sample) index.SeriesID {
	if id, ok := s.ids[key]; ok {
		return id
	}
	id := index.SeriesID(len(s.meta))
	labels := copyLabels(sample.Labels)
	s.meta = append(s.meta, seriesMeta{key: key, metric: sample.Metric, labels: labels})
	s.ids[key] = id
	s.index.Add(id, sample.Metric, labels)
	return id
}

func copyLabels(l model.Labels) model.Labels {
	out := make(model.Labels, len(l))
	for k, v := range l {
		out[k] = v
	}
	return out
}

// checkOrder reports whether the sample can be appended to its series
// A compressed chunk is an append-only stream, so a sample that is not newer than the last one cannot be stored
//
// The check uses the series' own watermark rather than its head chunk, because
// a flush empties the head: checking the chunk would let a sample older than
// already-flushed data back in, and queries would return two values for one
// timestamp.
func (s *MemoryStorage) checkOrder(sample model.Sample) error {
	key := model.SeriesKey(sample.Metric, sample.Labels)
	id, ok := s.ids[key]
	if !ok {
		return nil // a new series accepts any timestamp
	}
	if last := s.meta[id].lastTS; sample.Point.Timestamp <= last {
		return fmt.Errorf("%w: series %q got %d, last was %d",
			compress.ErrOutOfOrder, key, sample.Point.Timestamp, last)
	}
	return nil
}

func (s *MemoryStorage) Append(sample model.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkOrder(sample); err != nil {
		return err
	}

	line, err := json.Marshal(sample)
	if err != nil {
		return err
	}
	if _, err := s.wal.Write(append(line, '\n')); err != nil {
		return err
	}

	if s.syncOnWrite {
		if err := s.wal.Sync(); err != nil {
			return err
		}
	}

	if err := s.appendMemory(sample); err != nil {
		return err
	}

	if s.pointCount >= s.flushThreshold {
		if err := s.flush(); err != nil {
			return err
		}
	}

	return nil
}

func (s *MemoryStorage) flush() error {
	if len(s.series) == 0 {
		return nil
	}

	b := block{
		MinTS:  math.MaxInt64,
		MaxTS:  math.MinInt64,
		Series: make(map[string]blockSeries, len(s.series)),
	}
	for key, c := range s.series {
		if c.Count() == 0 {
			continue
		}
		tsBuf, valBuf := c.Bytes()
		b.Series[key] = blockSeries{Count: c.Count(), TS: tsBuf, Val: valBuf}
		if c.MinTS() < b.MinTS {
			b.MinTS = c.MinTS()
		}
		if c.MaxTS() > b.MaxTS {
			b.MaxTS = c.MaxTS()
		}
	}
	if len(b.Series) == 0 {
		return nil
	}

	seq := s.BlockSeq + 1
	name := fmt.Sprintf("%s%06d%s", blockFilePrefix, seq, blockFileSuffix)
	finalPath := filepath.Join(s.blockDir, name)
	tmpPath := finalPath + ".tmp"

	// 1. write tmp + fsync
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// atomic rename
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	// 2. fsync the directory so the rename metadata is durable
	if err := syncDir(s.blockDir); err != nil {
		return err
	}

	// 3. truncate the WAL (the block is now durable)
	if err := s.wal.Truncate(0); err != nil {
		return err
	}
	if _, err := s.wal.Seek(0, 0); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}

	// 4. clear the head
	s.series = make(map[string]*compress.Chunk)
	s.pointCount = 0
	s.BlockSeq = seq

	log.Printf("flushed block %s (min_ts=%d max_ts=%d), WAL truncated", name, b.MinTS, b.MaxTS)
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Query returns the samples in [start, end] for the one series identified
// exactly by metric and labels.
func (s *MemoryStorage) Query(metric string, labels model.Labels, start, end int64) ([]model.Point, error) {
	key := model.SeriesKey(metric, labels)
	s.mu.RLock()
	defer s.mu.RUnlock()

	points, err := s.readSeries([]string{key}, start, end)
	if err != nil {
		return nil, err
	}
	if points[key] == nil {
		return []model.Point{}, nil
	}
	return points[key], nil
}

// Series is one series matched by Select, with its samples in the range.
type Series struct {
	Metric string
	Labels model.Labels
	Points []model.Point
}

// Select returns every series carrying all of the matchers, with its samples
// in [start, end], ordered by series key. Series with no samples in the range
// are left out.
//
// The index is rebuilt from the WAL on startup but not yet from blocks, so a
// series whose samples all predate a restart is not found.
func (s *MemoryStorage) Select(matchers []index.Matcher, start, end int64) ([]Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.index.Match(matchers...)
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.meta[id].key
	}
	sort.Strings(keys) // stable output, independent of when series were first seen

	points, err := s.readSeries(keys, start, end)
	if err != nil {
		return nil, err
	}

	result := make([]Series, 0, len(keys))
	for _, key := range keys {
		if len(points[key]) == 0 {
			continue
		}
		m := s.meta[s.ids[key]]
		result = append(result, Series{
			Metric: m.metric,
			Labels: copyLabels(m.labels),
			Points: points[key],
		})
	}
	return result, nil
}

// readSeries decodes the samples in [start, end] for each series key, from the
// head and from every overlapping block, each series sorted by time. Blocks
// are read once for all keys rather than once per key.
func (s *MemoryStorage) readSeries(keys []string, start, end int64) (map[string][]model.Point, error) {
	out := make(map[string][]model.Point, len(keys))

	// in-memory head: the chunk is decoded from the start, since every sample depends on the one before it
	for _, key := range keys {
		c := s.series[key]
		if c == nil {
			continue
		}
		it := c.Iterator()
		for it.Next() {
			ts, v := it.At()
			if ts >= start && ts <= end {
				out[key] = append(out[key], model.Point{Timestamp: ts, Value: v})
			}
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("decode head chunk %q: %w", key, err)
		}
	}

	if err := s.readBlocks(out, keys, start, end); err != nil {
		return nil, err
	}

	for _, points := range out {
		sort.Slice(points, func(i, j int) bool {
			return points[i].Timestamp < points[j].Timestamp
		})
	}
	return out, nil
}

// readBlocks adds the samples in [start, end] for each key from on-disk
// blocks, reading only blocks whose time range overlaps (block pruning).
func (s *MemoryStorage) readBlocks(out map[string][]model.Point, keys []string, start, end int64) error {
	if len(keys) == 0 {
		return nil
	}
	entries, err := os.ReadDir(s.blockDir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, blockFilePrefix) || !strings.HasSuffix(name, blockFileSuffix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.blockDir, name))
		if err != nil {
			return err
		}
		var b block
		if err := json.Unmarshal(data, &b); err != nil {
			return err
		}
		// skip blocks entirely outside the query range
		if b.MaxTS < start || b.MinTS > end {
			continue
		}
		for _, key := range keys {
			bs, ok := b.Series[key]
			if !ok {
				continue
			}
			it := compress.NewChunkIterator(bs.TS, bs.Val, bs.Count)
			for it.Next() {
				ts, v := it.At()
				if ts >= start && ts <= end {
					out[key] = append(out[key], model.Point{Timestamp: ts, Value: v})
				}
			}
			if err := it.Err(); err != nil {
				return fmt.Errorf("decode block %s series %q: %w", name, key, err)
			}
		}
	}
	return nil
}

func (s *MemoryStorage) Close() error {
	return s.wal.Close()
}
