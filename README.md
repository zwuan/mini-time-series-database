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
- Gorilla-style compression: delta-of-delta timestamps + XOR float values,
  20–58x on typical metric shapes (see "Compression" below)

## Quick start
```bash
go run .
# in another terminal:
curl -X POST localhost:8080/write \
  -d '{"metric":"cpu","labels":{"region":"apac"},"point":{"timestamp":100,"value":1.5}}'
curl 'localhost:8080/query?metric=cpu&region=apac&start=0&end=1000'
```

Timestamps must increase per series; an older or duplicate timestamp is
rejected with `400`, since the head keeps one append-only compressed stream
per series.

Flags: `-addr` (default `:8080`), `-wal` (default `data/wal.log`), and
`-flush`, the number of samples buffered before a block is written. Lower it
to watch flushing happen:

```bash
go run . -flush 5
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

Values are encoded by **XOR-ing each float64 with the previous one**. Adjacent
measurements usually share their sign, exponent and high mantissa bits, so the
XOR has a run of leading zeros and often trailing zeros too; only the window
of meaningful bits in between is stored.

```
23.5  0100000000110111100000000000000000000000000000000000000000000000
23.6  0100000000110111100110011001100110011001100110011001100110011010
XOR   0000000000000000000110011001100110011001100110011001100110011010
      └─── 19 leading zeros ──┘└──────── meaningful window ──────────┘
```

| case | control bits | payload |
|---|---|---|
| value unchanged (XOR is 0) | `0` | — (**1 bit total**) |
| window fits inside the previous one | `10` | payload only |
| new window needed | `11` | 5-bit leading count + 6-bit width + payload |

A `Chunk` pairs the two encoders into one compressed stream per series. Because
both streams are append-only, **samples must arrive in strictly increasing
timestamp order** — out-of-order writes are rejected rather than buffered, the
same trade-off Prometheus makes.

## Performance

Measured on an M-series MacBook Pro, Go 1.22. Every number below is produced by
a test in the repo, so they can be reproduced rather than taken on trust.

### Compression, end to end
Bytes actually written to `data/blocks/`, including the JSON and base64
framing — not just the encoder output. 10,000 samples per case, raw being 16
bytes per sample (`int64` timestamp + `float64` value):

| Series shape | On disk | Raw | Ratio | Bits/sample |
|---|---|---|---|---|
| Constant value | 4,770 B | 160,000 B | **33.5x** | 3.8 |
| Integer gauge | 11,474 B | 160,000 B | **13.9x** | 9.2 |
| Monotonic counter | 25,774 B | 160,000 B | 6.2x | 20.6 |
| Accumulating float (`+= 0.1`) | 91,242 B | 160,000 B | 1.8x | 73.0 |
| Random float | 101,346 B | 160,000 B | 1.6x | 81.1 |

Measured against the encoders alone, without the block framing, the same
shapes reach 58.4x and 20.6x — the JSON/base64 wrapper costs roughly 40% of
the win, and replacing it with a binary block format is on the roadmap.
Timestamps on their own compress **56.7x** (1.13 bits/point) at a regular
interval.

**Why the spread is so wide.** Compression here is entirely a function of the
data. Integers are exact in float64, so their mantissas end in zeros and XOR
leaves long trailing runs. A value like `23.51` is not representable exactly
in binary, so its low mantissa bits are effectively noise and successive
values XOR to almost nothing compressible. Gorilla's headline ratios assume
the workload Facebook had: many constant and integer-valued series. A single
number without the series shape would be misleading.

### Write throughput
Full path per sample: order validation, WAL append, compression, and a
periodic flush.

| Flush threshold | Throughput | Per sample |
|---|---|---|
| every 100 samples | 7,003 points/sec | 142.8 µs |
| every 1,000 samples | 52,658 points/sec | 19.0 µs |
| every 10,000 samples | 216,529 points/sec | 4.6 µs |
| never flushed | 392,465 points/sec | 2.6 µs |

### Where the time goes
| Step | Cost |
|---|---|
| Gorilla encode (`Chunk.Append`) | 44 ns |
| Series key construction | 84 ns |
| WAL record marshalling | 325 ns |
| WAL write syscall | 1,681 ns |
| One flush (3 fsyncs) | ~14 ms |

A flush costs about 14 ms regardless of how much data it writes, because the
cost is three `fsync` calls — the block file, the block directory, and the
WAL. That is why throughput scales almost linearly with the flush threshold,
and why **compression is not the bottleneck: durability is.** Batching WAL
writes and fsyncs is the obvious next optimisation.

### Query latency
Decoding 10,000 samples for one series:

| Source | Latency |
|---|---|
| In-memory head chunk | 0.49 ms |
| 10 block files on disk | 0.84 ms |

A compressed stream has to be decoded from the start, so query cost grows with
chunk size. Capping chunk length (Prometheus uses 120 samples) would bound it.

### Reproducing
```bash
go test ./internal/storage/ -run 'TestEndToEndCompression|TestFlushThresholdTradeoff' -v
go test ./internal/storage/ -run XXX -bench . -benchtime 2s
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
- [x] Gorilla-style timestamp compression (delta-of-delta)
- [x] Gorilla-style value compression (float64 XOR)
- [x] Per-series compressed chunks
- [x] Wire compression into the storage layer
- [ ] Inverted index for label lookup
- [ ] Query engine (aggregation, rate, downsampling)

## Tech
Go, standard library only (no external dependencies yet).
