# MiniTSDB

A mini time-series database written in Go, built from scratch to explore how
observability platforms (Prometheus, InfluxDB, Datadog) store and query
metrics efficiently.

> Work in progress — built incrementally. See "Status" below.

## Features (so far)
- Ingest metrics (`metric` + `labels` + `timestamp` + `value`) via HTTP
- Label-based, time-range queries
- Write-ahead log (WAL) for crash-safe durability
- Automatic recovery: replays the WAL on startup

## Quick start
```bash
go run .
# in another terminal:
curl -X POST localhost:8080/write \
  -d '{"metric":"cpu","labels":{"region":"apac"},"point":{"timestamp":100,"value":1.5}}'
curl 'localhost:8080/query?metric=cpu&region=apac&start=0&end=1000'
```

## Status / Roadmap
- [x] In-memory store + HTTP API
- [x] WAL persistence & recovery
- [ ] On-disk blocks + compaction
- [ ] Gorilla-style compression
- [ ] Inverted index for label lookup
- [ ] Query engine (aggregation, rate, downsampling)

## Tech
Go, standard library only (no external dependencies yet).
