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
- Gorilla-style delta-of-delta timestamp compression (56.7x on a regular
  scrape interval — see "Compression" below)

## Quick start
```bash
go run .
# in another terminal:
curl -X POST localhost:8080/write \
  -d '{"metric":"cpu","labels":{"region":"apac"},"point":{"timestamp":100,"value":1.5}}'
curl 'localhost:8080/query?metric=cpu&region=apac&start=0&end=1000'
```

## Compression (Gorilla-style)
Timestamps are encoded with **delta-of-delta**: instead of storing each
timestamp, store how much the *interval* changed. Metrics are usually scraped
on a fixed schedule, so that change is almost always zero — and zero costs a
single bit.

```
timestamps   1600000000   1600000010   1600000020   1600000030
delta                 —           10           10           10
delta-of-delta        —            —            0            0   ← 1 bit each
```

Non-zero values fall into progressively wider buckets behind a prefix-free
control code, so a decoder always knows how many bits to read next:

| delta-of-delta | control bits | payload | total |
|---|---|---|---|
| `0` | `0` | — | **1 bit** |
| `[-64, 63]` | `10` | 7 bits | 9 bits |
| `[-256, 255]` | `110` | 9 bits | 12 bits |
| `[-2048, 2047]` | `1110` | 12 bits | 16 bits |
| anything else | `1111` | 64 bits | 68 bits |

The first timestamp and the first delta are stored raw (64 bits each), which
is why the ratio improves with block size — the header cost is amortised.

### Measured
| Input | Encoded | Raw | Ratio | Bits/point |
|---|---|---|---|---|
| 1000 timestamps, regular 10s interval | 141 B | 8000 B | **56.7x** | 1.13 |
| 5 timestamps, regular 10s interval | 17 B | 40 B | 2.4x | 27.2 |

Reproduce with:
```bash
go test ./internal/compress/ -run TestTimestampCompressionRatio -v
```

> Value (float64) XOR encoding is next; the numbers above cover timestamps
> only, and the encoder is not yet wired into the storage layer.

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
- [x] Gorilla-style timestamp compression (delta-of-delta)
- [ ] Gorilla-style value compression (float64 XOR)
- [ ] Wire compression into the storage layer
- [ ] Inverted index for label lookup
- [ ] Query engine (aggregation, rate, downsampling)

## Tech
Go, standard library only (no external dependencies yet).
