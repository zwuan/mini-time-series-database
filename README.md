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
- Disk tiering: the in-memory head is periodically flushed into immutable
  block files, so memory does not grow unbounded
- WAL checkpointing: once a block is durable, the WAL is truncated so it
  stays small
- Queries transparently merge the in-memory head with on-disk blocks

## Quick start
```bash
go run .
# in another terminal:
curl -X POST localhost:8080/write \
  -d '{"metric":"cpu","labels":{"region":"apac"},"point":{"timestamp":100,"value":1.5}}'
curl 'localhost:8080/query?metric=cpu&region=apac&start=0&end=1000'
```

## How it works (storage)
```
write ──▶ WAL (append) ──▶ in-memory head
                               │  when the head reaches flushThreshold
                               ▼
                         flush to an immutable block file
                         (fsync ▶ atomic rename ▶ fsync dir
                          ▶ truncate WAL ▶ clear head)

query ──▶ merge( in-memory head , on-disk blocks )
          blocks store min_ts/max_ts so out-of-range ones are skipped
```
On startup the WAL replays only the samples written since the last
checkpoint; already-flushed blocks stay on disk and are read at query time.

## Status / Roadmap
- [x] In-memory store + HTTP API
- [x] WAL persistence & recovery
- [x] On-disk block flushing + WAL checkpointing
- [ ] Block compaction (merge small blocks)
- [ ] Gorilla-style compression
- [ ] Inverted index for label lookup
- [ ] Query engine (aggregation, rate, downsampling)

## Tech
Go, standard library only (no external dependencies yet).
