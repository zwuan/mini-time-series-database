package storage

import (
	"encoding/json"
	"path/filepath"
	"os"
	"sort"
	"sync"
	"bufio"
	"log"

	"minitsdb/internal/model"
)

type MemoryStorage struct {
	mu   sync.RWMutex
	series map[string][]model.Point

	wal *os.File
}

func NewMemoryStorage(walPath string) (*MemoryStorage, error) {
	if err := os.MkdirAll(filepath.Dir(walPath), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	s := &MemoryStorage{
		series: make(map[string][]model.Point),
		wal: f,
	}
	
	if err := s.replay(walPath); err != nil {
		f.Close()
		return nil, err
	}

	return s, nil
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
	
	key := model.SeriesKey(sample.Metric, sample.Labels)
	points := append(s.series[key], sample.Point)
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	
	s.series[key] = points

	return nil
}

func (s *MemoryStorage) Query(metric string, labels model.Labels, start, end int64) []model.Point {
	key := model.SeriesKey(metric, labels)
	s.mu.RLock()
	defer s.mu.RUnlock()

	all := s.series[key]
	result := make([]model.Point, 0)
	for _, p := range all {
		if p.Timestamp >= start && p.Timestamp <= end {
			result = append(result, p)
		}
	}
	return result
}

func (s *MemoryStorage) Close() error {
	return s.wal.Close()
}