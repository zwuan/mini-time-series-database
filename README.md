# MiniTSDB

A mini time-series database written in Go, built from scratch to explore how
observability platforms (Prometheus, InfluxDB, Datadog) store and query
metrics efficiently.

> Work in progress — built incrementally. See "Status" below.

## Features (so far)
- Ingest metrics (`metric` + `labels` + `timestamp` + `value`) via HTTP
- Time-range queries by exact series, or by any subset of labels through an
  inverted index (`GET /select`)
- Aggregation in Datadog's two-stage model: roll each series up into time
  buckets (`avg`, `sum`, `min`, `max`, `count`, `last`, `rate`), then combine
  series bucket by bucket, optionally grouped by label (`GET /aggregate`)
- Write-ahead log (WAL) for crash-safe durability, with optional per-write
  fsync (`-sync`)
- Automatic recovery: rebuilds the label index from blocks and replays the WAL
  on startup
- Disk tiering: the in-memory head is periodically flushed into immutable
  block files, so memory does not grow unbounded
- WAL checkpointing: once a block is durable, the WAL is truncated so it
  stays small
- Queries transparently merge the in-memory head with on-disk blocks
- Gorilla-style compression: delta-of-delta timestamps + XOR float values,
  13–29x on disk for integer and constant-valued series (see "Performance")

## Quick start
```bash
go run .
# in another terminal:
curl -X POST localhost:8080/write \
  -d '{"metric":"cpu","labels":{"region":"apac"},"point":{"timestamp":100,"value":1.5}}'
curl 'localhost:8080/query?metric=cpu&region=apac&start=0&end=1000'
```

The two read endpoints answer different questions:

| Endpoint | Returns | `?metric=cpu&region=apac` matches `cpu{host="a",region="apac"}`? |
|---|---|---|
| `GET /query` | the points of the one series whose labels are **exactly** the ones given | no — the series also has `host` |
| `GET /select` | every series carrying **at least** the given labels, each with its labels and points | yes |

```bash
curl 'localhost:8080/select?metric=cpu&region=apac'
# [{"metric":"cpu","labels":{"region":"apac"},"points":[{"timestamp":100,"value":1.5}]}]
```

`/select` treats the metric like any other label, so it can be left out
(`/select?region=apac` finds every metric in that region). A select with no
metric and no labels is rejected with `400` rather than returning everything.

Timestamps must increase per series; an older or duplicate timestamp is
rejected with `400`, since the head keeps one append-only compressed stream
per series.

Flags: `-addr` (default `:8080`), `-wal` (default `data/wal.log`), `-flush`
(samples buffered before a block is written), and `-sync` (fsync the WAL on
every write — durable but far slower; see "Durability" below). Lower the flush
threshold to watch flushing happen:

```bash
go run . -flush 5
```

### Docker
The image is a static binary on a distroless base, running as a non-root user,
with the WAL and blocks in a volume at `/data`:

```bash
docker build -t minitsdb .
docker run --rm -p 8080:8080 -v minitsdb-data:/data minitsdb
```

Flags go after the image name, for example `docker run --rm -p 8080:8080 -v
minitsdb-data:/data minitsdb -flush 5`. The named volume outlives the
container, so stopping and starting it exercises the same recovery path as
restarting the binary. `docker stop` ends the process but not the machine,
so every acknowledged write survives it even without `-sync` (see
"Durability").

## Aggregation
`/aggregate` evaluates a query the way Datadog does, in two stages:

1. **Rollup (time aggregation).** Each matching series is cut into buckets of
   `step` time units and every bucket is reduced to one value. This is also
   how a wide range is downsampled: a week of 10-second samples queried with
   `step=3600` comes back as 168 points per series.
2. **Space aggregation** (optional, `agg`). The rolled-up series are combined
   bucket by bucket, either all together or grouped by the labels in `by`.

A Datadog query such as `sum:cpu{region:apac} by {host}.rollup(avg, 60)` maps to:

```
GET /aggregate?metric=cpu&region=apac&step=60&rollup=avg&agg=sum&by=host
```

| Parameter | Meaning | Values |
|---|---|---|
| `step` | bucket width, required | positive integer, in timestamp units |
| `rollup` | how one series is reduced within a bucket | `avg` (default), `sum`, `min`, `max`, `count`, `last`, `rate` |
| `agg` | how series are combined within a bucket | omit for none, or `sum`, `avg`, `min`, `max`, `count` |
| `by` | labels to group by when aggregating | comma-separated; requires `agg` |

With three hosts reporting a cpu gauge every 30s:

```bash
curl 'localhost:8080/aggregate?metric=cpu&step=60&rollup=avg&agg=sum&by=region'
# [{"labels":{"region":"apac"},"points":[{"timestamp":0,"value":32},{"timestamp":60,"value":56}]},
#  {"labels":{"region":"us"},"points":[{"timestamp":0,"value":6},{"timestamp":60,"value":10}]}]
```

Aggregated series carry only their `by` labels and no metric, since a sum
across series is no longer any one of them.

Semantics worth knowing:

- **Buckets align to multiples of `step`**, not to the query's `start`, so a
  sample falls in the same bucket however the range is chosen. Only buckets
  that receive data are returned, so the response never outgrows the samples
  behind it.
- **`rate` handles counter resets.** A value lower than the one before means
  the counter restarted from zero, so its new value counts as the increase
  rather than producing a negative rate. Each increase is credited to the
  bucket of the later sample, so increases across a bucket boundary are kept;
  the first sample in the range has no predecessor and contributes nothing.
- **Reserved names.** `step`, `rollup`, `agg` and `by` are parameters of
  `/aggregate`, so labels with those names cannot be matched there.
- There is no query language: the query is expressed in URL parameters, not
  parsed from a string like `sum(rate(cpu[1m])) by (region)`.

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

## Label lookup (inverted index)
Every series is identified by its metric and labels. The index keeps, for each
`label=value` pair, a sorted list of the series carrying it — a posting list.
The metric name is indexed as one more label, `__name__`, so selecting by
metric is not a special case:

```
__name__=cpu    → [1, 2, 3]
region=apac     → [1, 2]
host=a          → [1, 4]

/select?metric=cpu&region=apac  =  [1, 2, 3] ∩ [1, 2]  =  [1, 2]
```

A query intersects the posting lists of its matchers, with two optimisations:

- **Shortest list first.** An intersection can only shrink, so starting from
  the most selective matcher keeps every later pass small.
- **Galloping search.** When one list is far longer than the other, the merge
  probes ahead at doubling offsets and binary-searches the bracket it lands
  in, rather than stepping through every element.

A series keeps its ID and index entries when its samples are flushed, and
every block records the metric and labels of the series it holds, so the
index is rebuilt from blocks on startup. Matching is equality only;
`!=` and regular-expression matchers are not supported.

## Performance

Measured on an M-series MacBook Pro, Go 1.26. Every number below is produced by
a test in the repo, so they can be reproduced rather than taken on trust.

### Compression, end to end
Bytes actually written to `data/blocks/`, including the JSON and base64
framing — not just the encoder output. 10,000 samples per case, raw being 16
bytes per sample (`int64` timestamp + `float64` value):

| Series shape | On disk | Raw | Ratio | Bits/sample |
|---|---|---|---|---|
| Constant value | 5,500 B | 160,000 B | **29.1x** | 4.4 |
| Integer gauge | 12,204 B | 160,000 B | **13.1x** | 9.8 |
| Monotonic counter | 26,504 B | 160,000 B | 6.0x | 21.2 |
| Accumulating float (`+= 0.1`) | 91,972 B | 160,000 B | 1.7x | 73.6 |
| Random float | 102,096 B | 160,000 B | 1.6x | 81.7 |

Measured against the encoders alone, without the block framing, the same
shapes reach 58.4x and 20.6x. The framing — JSON, base64, and the metric and
labels each block stores per series so the index can be rebuilt — roughly
halves the ratio for the constant series and costs about a third for the
integer gauge. It is a fixed cost per series per block, so it shrinks as
blocks grow; a binary block format would cut most of it.
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

### Durability: what `Write` does and does not promise
By default the WAL is appended to but not fsynced per sample, so a freshly
written sample lives in the OS page cache until the next flush. A process
crash loses nothing — the file is already written — but a power loss drops
whatever arrived since the last flush.

`-sync` closes that window by fsyncing on every append. It is off by default
because of what it costs:

| Mode | Throughput | Per sample |
|---|---|---|
| WAL buffered (default) | 228,022 points/sec | 4.4 µs |
| `-sync`, fsync every append | 257 points/sec | 3,890 µs |

**887x slower**, because each fsync waits on the physical device (~3.9 ms).
Prometheus makes the same default choice: for metrics, losing a few seconds of
samples on power loss beats an ingest path that can absorb only a few hundred
points per second. Workloads that cannot tolerate the gap can pay for it
explicitly. Group commit — fsyncing once per batch on a timer — would be the
middle ground, and is the natural next step.

### Query latency
Decoding 10,000 samples for one series:

| Source | Latency |
|---|---|
| In-memory head chunk | 0.49 ms |
| 10 block files on disk | 0.84 ms |

A compressed stream has to be decoded from the start, so query cost grows with
chunk size. Capping chunk length (Prometheus uses 120 samples) would bound it.

### Posting-list intersection
Each optimisation against the naive version on the same input:

| Case | Naive | Optimised | Speedup |
|---|---|---|---|
| 10 series ∩ 100k series | linear merge: 102 µs | galloping: 0.33 µs | ~310x |
| 100k ∩ 10k ∩ 10 series | in given order, linear: 136 µs | shortest first, galloping: 0.52 µs | ~260x |

### Reproducing
```bash
go test ./internal/storage/ -run 'TestEndToEndCompression|TestFlushThresholdTradeoff|TestSyncOnWriteCost' -v
go test ./internal/storage/ -run XXX -bench . -benchtime 2s
go test ./internal/index/ -run XXX -bench BenchmarkIntersectOptimizations
```

## How it works (storage)
```
write ──▶ WAL (append) ──▶ in-memory head
                               │  when the head reaches flushThreshold
                               ▼
                         flush to an immutable block file
                         (fsync ▶ atomic rename ▶ fsync dir
                          ▶ truncate WAL ▶ clear head)

query ──▶ inverted index ──▶ matching series
          ──▶ merge( in-memory head , on-disk blocks ) per series
          blocks store min_ts/max_ts so out-of-range ones are skipped
          ──▶ rollup per series ──▶ aggregate across series   (/aggregate)
```
On startup, each block's series identities (metric, labels, latest timestamp)
are read back to rebuild the index and the ordering check; the samples
themselves stay on disk until a query needs them. The WAL then replays only
the samples written since the last checkpoint.

## Design decisions
Each choice below has a cost; the trade-off is the point.

**Out-of-order samples are rejected, not buffered.** A Gorilla stream encodes
each sample relative to the one before it, so a sample cannot be inserted into
the middle of a chunk without re-encoding everything after it. Accepting only
increasing timestamps keeps the head as one compressed stream per series, as
Prometheus does. Late data is lost unless the sender orders it; a small
uncompressed buffer, or a separate out-of-order head merged at query time,
would accept it at the price of memory and a more complex read path.

**Ordering is checked before the WAL write.** A sample that will be rejected
must never enter the log, or replay would hit the same rejection and the log
would describe a state the store cannot reach. The check reads a per-series
watermark kept with the series' identity rather than the head chunk, because
a flush empties the head; an earlier version read the chunk and accepted
stale samples after every flush.

**The WAL is not fsynced per write by default.** A process crash loses
nothing, since written data is already in the OS page cache; only a power loss
drops what arrived since the last flush. Fsyncing every append costs 887x
throughput, so it is opt-in with `-sync`, the default Prometheus also picks.
Group commit would be the middle ground.

**A flush is ordered so that a crash never loses data.** The block is written
to a temporary file, fsynced, renamed into place, and its directory fsynced,
all before the WAL is truncated. Truncating first would open a window in which
the samples exist nowhere. The opposite window, a crash after the rename but
before the truncate, leaves the samples in both; replay skips any sample at or
before its series' watermark, which startup has already restored from the
blocks, so nothing is stored twice.

**Series identity lives apart from series data.** A series' ID, labels, index
entries and ordering watermark stay in memory when its samples are flushed,
and every block records the identity of the series it holds. Flushes move
data, not identity, so lookups and ordering survive flushes and restarts.

**The metric name is just a label.** Indexing it as `__name__` makes matching
on a metric and on a label the same operation, which is also why `/select` can
leave the metric out.

**Matchers are equality only.** Equality covers what the inverted index is
for, posting-list intersection, and keeps each matcher a single lookup. `!=`
needs every series minus a posting list, and a regular expression needs a
scan of every value of a label; both were left out.

**Queries roll up, then aggregate.** Following Datadog's model, each series is
reduced to buckets before series are combined. Gauges and counters (through
`rate`) share one evaluation path, and downsampling is the same operation as
a rollup. Buckets align to multiples of `step` and empty ones are omitted, so
results do not shift with the query range and never outgrow their data.

**Blocks are JSON.** They can be read with `cat` and needed no format code,
which mattered while the layout kept changing. The cost is measured: base64
and JSON keys roughly halve the compression ratio of a constant series. A
binary format is the first thing to change once the layout settles.

**No block format version.** The one format change so far that still parsed,
blocks gaining series identity, is caught by checking that every series has a
metric, which every write requires. A version field earns its keep once a
change can parse cleanly while meaning something different.

**Failures are reported, not absorbed.** A block that fails to decode fails
the query instead of returning whatever did decode; an unparseable `start` is
a `400`, not a silent default; an unconstrained `/select` is refused instead
of returning every series. An incomplete answer that looks complete is the
hardest failure to notice downstream.

**Standard library only.** Every piece, from the bit writer to the index, is in
this repository, which is the purpose of the project.

## Status / Roadmap
- [x] In-memory store + HTTP API
- [x] WAL persistence & recovery
- [x] On-disk block flushing + WAL checkpointing
- [x] Configurable durability and flush thresholds
- [ ] Group commit (batched WAL fsync)
- [ ] Block compaction (merge small blocks)
- [ ] Binary block format
- [x] Gorilla-style timestamp compression (delta-of-delta)
- [x] Gorilla-style value compression (float64 XOR)
- [x] Per-series compressed chunks
- [x] Wire compression into the storage layer
- [x] Inverted index for label lookup (`GET /select`), rebuilt from blocks on
  startup
- [x] Query engine: rollups (incl. `rate` and downsampling) and aggregation
  across series, grouped by label (`GET /aggregate`)
- [ ] Query language parser
- [x] Container image (Docker, distroless, non-root)

## Tech
Go, standard library only (no external dependencies yet).
