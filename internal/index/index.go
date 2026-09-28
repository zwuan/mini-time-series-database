package index

import (
	"sort"
)

const MetricName = "__name__"

// SeriesID identifies a series in the index
type SeriesID uint32

type Matcher struct {
	Name  string
	Value string
}

type Index struct {
	postings map[string][]SeriesID
}

func New() *Index {
	return &Index{
		postings: make(map[string][]SeriesID),
	}
}

func (idx *Index) Add(id SeriesID, metric string, labels map[string]string) {
	idx.addPosting(key(MetricName, metric), id)
	for name, value := range labels {
		idx.addPosting(key(name, value), id)
	}
}

func (idx *Index) addPosting(k string, id SeriesID) {
	list := idx.postings[k]
	if n := len(list); n == 0 || list[n-1] < id {
		idx.postings[k] = append(list, id)
		return
	}
	i := sort.Search(len(list), func(i int) bool { return list[i] >= id })
	if i < len(list) && list[i] == id {
		return
	}
	list = append(list, 0)
	copy(list[i+1:], list[i:])
	list[i] = id
	idx.postings[k] = list
}

func (idx *Index) Postings(name, value string) []SeriesID {
	return idx.postings[key(name, value)]
}

func (idx *Index) Match(matchers ...Matcher) []SeriesID {
	if len(matchers) == 0 {
		return nil
	}

	lists := make([][]SeriesID, 0, len(matchers))

	for _, m := range matchers {
		list := idx.Postings(m.Name, m.Value)
		if len(list) == 0 {
			return nil
		}
		lists = append(lists, list)
	}
	return Intersect(lists)
}

func (idx *Index) Cardinality(name, value string) int {
	return len(idx.Postings(name, value))
}

func key(name, value string) string {
	return name + "=" + value
}

func Intersect(lists [][]SeriesID) []SeriesID {
	if len(lists) == 0 {
		return nil
	}

	ordered := make([][]SeriesID, len(lists))
	copy(ordered, lists)
	sort.Slice(ordered, func(i, j int) bool {
		return len(ordered[i]) < len(ordered[j])
	})

	result := append([]SeriesID(nil), ordered[0]...)
	for _, list := range ordered[1:] {
		result = intersectTwo(result, list)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

func intersectTwo(a, b []SeriesID) []SeriesID {
	out := a[:0]
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i = gallop(a, i, b[j])
		default:
			j = gallop(b, j, a[i])
		}
	}
	return out
}

func gallop(list []SeriesID, from int, target SeriesID) int {
	step := 1
	i := from
	for i < len(list) && list[i] < target {
		i += step
		step *= 2
	}
	lo := i - step/2
	if lo < from {
		lo = from
	}
	hi := i
	if hi > len(list) {
		hi = len(list)
	}
	return sort.Search(hi-lo, func(i int) bool { return list[lo+i] >= target }) + lo
}
