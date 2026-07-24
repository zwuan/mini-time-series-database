package storage

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"bufio"
	"log"

	"minitsdb/internal/model"
)

const blockFilePrefix = "block-"
const blockFileSuffix = ".json"

type MemoryStorage struct {
	mu   sync.RWMutex
	series map[string][]model.Point
	wal *os.File

	walPath string
	blockDir string
	BlockSeq int
	pointCount int
	flushThreshold int
}

// block is the immutable on-disk block file format.
// min_ts / max_ts let queries skip irrelevant blocks (block pruning).
type block struct {
	MinTS  int64                    `json:"min_ts"`
	MaxTS  int64                    `json:"max_ts"`
	Series map[string][]model.Point `json:"series"`
}

func NewMemoryStorage(walPath string) (*MemoryStorage, error) {
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
		series: make(map[string][]model.Point),
		wal: f,
		walPath: walPath,
		blockDir: blockDir,
		BlockSeq: 0,
		pointCount: 0,
		flushThreshold: 5, // flush threshold, set to 5 points for example
	}

	// Continue numbering from existing blocks so a restart never overwrites old ones.
	seq, err := scanMaxBlockSeq(blockDir)
	if err != nil {
		f.Close()
		return nil, err
	}
	s.BlockSeq = seq

	// Blocks stay on disk and are not loaded back; the WAL only holds samples
	// written since the last checkpoint.
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
		s.appendMemory(sample)
		count++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	log.Printf("WAL replay done: restored %d samples", count)
	return nil
}

func (s *MemoryStorage) appendMemory(sample model.Sample) {
	key := model.SeriesKey(sample.Metric, sample.Labels)
	points := append(s.series[key], sample.Point)
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	s.series[key] = points
	s.pointCount++
}


func (s *MemoryStorage) Append(sample model.Sample) error{
	line, err := json.Marshal(sample)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.wal.Write(append(line, '\n')); err != nil {
		return err
	}

	s.appendMemory(sample)

	if s.pointCount >= s.flushThreshold {
		if err := s.flush(); err != nil {
			return err
		}
	}

	return nil
}

// flush persists the current in-memory data as one immutable block file, then
// truncates the WAL and clears the head. The caller must already hold s.mu.
//
// The ordering is crash-safe and must not be reordered:
//   1. write block to tmp -> fsync -> rename (atomic)
//   2. fsync the directory
//   3. truncate the WAL (checkpoint)
//   4. clear the head, reset pointCount, bump BlockSeq
// Truncating the WAL before the block is durable would lose data on a crash.
func (s *MemoryStorage) flush() error {
	if len(s.series) == 0 {
		return nil
	}

	b := block{
		MinTS:  int64(^uint64(0) >> 1),  // max int64
		MaxTS:  -1 << 63,                // min int64
		Series: s.series,
	}
	for _, points := range s.series {
		for _, p := range points {
			if p.Timestamp < b.MinTS {
				b.MinTS = p.Timestamp
			}
			if p.Timestamp > b.MaxTS {
				b.MaxTS = p.Timestamp
			}
		}
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
	s.series = make(map[string][]model.Point)
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

func (s *MemoryStorage) Query(metric string, labels model.Labels, start, end int64) []model.Point {
	key := model.SeriesKey(metric, labels)
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]model.Point, 0)

	// in-memory head
	for _, p := range s.series[key] {
		if p.Timestamp >= start && p.Timestamp <= end {
			result = append(result, p)
		}
	}

	// on-disk blocks
	blockPoints, err := s.queryBlocks(key, start, end)
	if err != nil {
		log.Printf("query blocks failed: %v", err)
	} else {
		result = append(result, blockPoints...)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})
	return result
}

// queryBlocks scans on-disk blocks, reading only those whose time range overlaps (block pruning).
func (s *MemoryStorage) queryBlocks(key string, start, end int64) ([]model.Point, error) {
	entries, err := os.ReadDir(s.blockDir)
	if err != nil {
		return nil, err
	}

	result := make([]model.Point, 0)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, blockFilePrefix) || !strings.HasSuffix(name, blockFileSuffix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.blockDir, name))
		if err != nil {
			return nil, err
		}
		var b block
		if err := json.Unmarshal(data, &b); err != nil {
			return nil, err
		}
		// skip blocks entirely outside the query range
		if b.MaxTS < start || b.MinTS > end {
			continue
		}
		for _, p := range b.Series[key] {
			if p.Timestamp >= start && p.Timestamp <= end {
				result = append(result, p)
			}
		}
	}
	return result, nil
}

func (s *MemoryStorage) Close() error {
	return s.wal.Close()
}
