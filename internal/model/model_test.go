package model

import "testing"

func TestSeriesKeyIsOrderIndependent(t *testing.T) {
	a := SeriesKey("cpu", Labels{"region": "apac", "service": "api"})
	b := SeriesKey("cpu", Labels{"service": "api", "region": "apac"})

	if a != b {
		t.Errorf("series key should not depend on label order:%q vs %q", a, b)
	}
}

func TestSeriesKeyDifferentLabels(t *testing.T) {
	a := SeriesKey("cpu", Labels{"region": "apac"})
	c := SeriesKey("cpu", Labels{"region": "us"})

	if a == c {
		t.Errorf("different regions should produce different series keys, but they are the same:%q", a)
	}
}