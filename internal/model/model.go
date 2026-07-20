package model

import  (
	"sort"
	"strings"
)

type Labels map[string]string

type Sample struct {
	Metric string `json:"metric"`
	Labels Labels `json:"labels"`
	Point Point	`json:"point"`
}
type Point struct {
	Timestamp int64	`json:"timestamp"`
	Value     float64 `json:"value"`
}

func SeriesKey(metric string, labels Labels) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	var sb strings.Builder
	sb.WriteString(metric)
	for _, k := range keys {
		sb.WriteString(",")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(labels[k])
	}
	return sb.String()
}

