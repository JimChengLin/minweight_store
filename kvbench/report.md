# KVBench Report

This report records the first cross-engine benchmark pass for
`minweight_store`. It is a laptop benchmark, not a universal claim about every
filesystem or machine.

## Environment

- Date: 2026-06-01
- Machine: Apple M1 Pro
- OS/arch: darwin/arm64
- CPU limit: `GOMAXPROCS=4`
- SSD budget: `-max-data=100GB`
- Memory rule: RSS is reported, not enforced, on macOS because RSS includes
  file-backed mmap and page-cache pages. On Linux, `kvbenchrun` uses
  `/proc/<pid>/smaps_rollup` `Anonymous` bytes as the enforceable memory limit.
- `minweight_store` options: defaults, including 128MiB WAL, 512MiB target SST,
  `MaxImmutableWALNum=1`, default disk `LOG` enabled.
- Durability settings are the current kvbench defaults for each engine. The
  benchmark is intended to compare embedded-engine behavior under the same
  harness, not to prove equivalent crash-durability contracts.

## Workloads

The benchmark uses two tiers.

Default tier:

- Key size: 9 bytes
- Value size: 256 bytes
- Key count: 100,000
- Engines: `minweight`, `badger`, `bbolt`, `buntdb`, `goleveldb`, `pebble`
- Workloads:
  - `BenchmarkLoad`: pure insert of 100k new keys, run with `-benchtime=1x`.
  - `BenchmarkOverwrite`: preload 100k keys, then overwrite existing keys.
  - `BenchmarkGet`: preload 100k keys, then point reads.
  - `BenchmarkMixedReadWrite`: preload 100k keys, then 90% reads / 10% writes.
  - `BenchmarkScan`: ordered full scan of 100k keys.
  - `BenchmarkSeekGE`: ordered seek on the 100k-key set.

Large tier:

- Key size: 19 bytes
- Value size: 2KiB
- Entry count: 6,000,000
- Raw value bytes: 12.29GB
- Workload: `BenchmarkLargeLoad`, run with `-benchtime=1x`.
- This tier is meant to exceed the 10GB memory budget and exercise WAL, flush,
  minor compaction, SST write path, and final on-disk footprint.

## Commands

Default load:

```sh
env GOCACHE=/private/tmp/go-build-minweight-kvbench \
go run ./cmd/kvbenchrun \
  -bench '^BenchmarkLoad/(minweight|badger|bbolt|buntdb|goleveldb|pebble)$' \
  -benchtime=1x \
  -count=1 \
  -gomaxprocs=4 \
  -max-rss=10GiB \
  -max-data=100GB \
  -sample-rate=1s \
  -out results/eval-default-load
```

Default steady-state operations:

```sh
env GOCACHE=/private/tmp/go-build-minweight-kvbench \
go run ./cmd/kvbenchrun \
  -bench '^(BenchmarkOverwrite|BenchmarkGet|BenchmarkMixedReadWrite|BenchmarkScan|BenchmarkSeekGE)/(minweight|badger|bbolt|buntdb|goleveldb|pebble)$' \
  -benchtime=3s \
  -count=1 \
  -gomaxprocs=4 \
  -max-rss=10GiB \
  -max-data=100GB \
  -sample-rate=1s \
  -out results/eval-default-steady
```

Large load:

```sh
env GOCACHE=/private/tmp/go-build-minweight-kvbench \
go run ./cmd/kvbenchrun \
  -bench '^BenchmarkLargeLoad/(minweight|badger|bbolt|buntdb|goleveldb|pebble)$' \
  -benchtime=1x \
  -count=1 \
  -entries=6000000 \
  -value-size=2048 \
  -gomaxprocs=4 \
  -max-rss=10GiB \
  -max-data=100GB \
  -sample-rate=1s \
  -out results/eval-large-load-6m-2k
```

## Results

### Default Load

| Engine | Time | Throughput | Entries/s |
| --- | ---: | ---: | ---: |
| minweight | 50.7ms | 505.30 MB/s | 1,973,840 |
| pebble | 226.0ms | 113.29 MB/s | 442,540 |
| buntdb | 548.8ms | 46.65 MB/s | 182,227 |
| badger | 604.6ms | 42.34 MB/s | 165,396 |
| goleveldb | 884.0ms | 28.96 MB/s | 113,126 |
| bbolt | 4.34s | 5.90 MB/s | 23,032 |

### Default Steady-State Operations

| Workload | minweight | Best non-minweight | Notes |
| --- | ---: | ---: | --- |
| Overwrite | 765.9ns/op | pebble 2.238us/op | minweight is fastest in this run. |
| Get | 167.3ns/op | buntdb 257.9ns/op | minweight is fastest in this run. |
| Mixed 90R/10W | 419.1ns/op | buntdb 907.5ns/op | minweight is fastest in this run. |
| SeekGE | 246.0ns/op | buntdb 323.3ns/op | minweight is fastest in this run. |
| Scan 100k | 11.6ms | bbolt 1.39ms | minweight scan is the clear weak spot. |

Full scan numbers:

| Engine | Time per 100k scan | Entries/s | Allocated bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| bbolt | 1.39ms | 71,979,860 | 77,544 | 9,629 |
| buntdb | 9.53ms | 10,488,547 | 27,204,093 | 200,003 |
| minweight | 11.6ms | 8,609,021 | 27,200,032 | 200,002 |
| pebble | 21.0ms | 4,755,785 | 2,482 | 40 |
| goleveldb | 23.6ms | 4,231,255 | 3,252,173 | 40,549 |
| badger | 36.6ms | 2,732,903 | 1,672,321 | 100,521 |

### Large Load

| Engine | Time | Throughput | Entries/s | Approx. directory increment |
| --- | ---: | ---: | ---: | ---: |
| minweight | 31.9s | 385.63 MB/s | 188,296 | 13.13GB |
| goleveldb | 191.4s | 64.21 MB/s | 31,354 | 8.59GB |
| badger | 193.9s | 63.37 MB/s | 30,940 | 15.46GB |
| pebble | 227.5s | 54.00 MB/s | 26,370 | 12.56GB |
| bbolt | 358.6s | 34.27 MB/s | 16,733 | 29.22GB |
| buntdb | 514.0s | 23.91 MB/s | 11,673 | 17.12GB |

The directory increment column is reconstructed from one-second
`resource_samples.csv` around each benchmark completion time. It is approximate.
The raw value size is 12.29GB, so minweight's large-load footprint is close to
raw data size plus index/WAL/SST overhead.

Large-load run summary:

- Wall time: 1530.10s
- Final data directory size before runner cleanup: 96.72GB
- Peak data directory size: 96.72GB
- Peak sampled RSS: 14.76GB
- Data limit exceeded: no

## Observations

- `minweight_store` is strong on write-heavy and point-operation workloads in
  this benchmark. It leads default load, overwrite, get, mixed read/write, and
  seek.
- `minweight_store` large-load throughput is substantially higher than the
  other pure-Go engines tested here.
- `Scan` is the main current weakness. On the 100k-key scan, bbolt is about
  8.4x faster than minweight. The minweight scan path still allocates heavily
  because public API safety clones returned key/value bytes.
- bbolt has excellent scan performance but poor large-load write throughput and
  high disk footprint in this workload.
- buntdb is acceptable on some small in-memory-friendly operations but is not a
  good candidate for the large tier. In large-read attempts, its preload stage
  was the long pole and wrote both `db.buntdb` and `db.buntdb.tmp`.
- goleveldb has the smallest approximate large-load footprint in this run, but
  much lower write throughput than minweight.

## Limitations

- RSS is not an enforced memory limit on macOS. The reported peak RSS includes
  file-backed mmap and page-cache pages.
- `iostat` is device-level for the whole machine, not per-process attribution.
- Large `BenchmarkLargeGet` was started but stopped after identifying buntdb as
  the slow path. The existing Go benchmark shape repeatedly rebuilds large
  databases during calibration, making it too expensive for regular use.
- Directory sizes in multi-engine runs are benchmark-harness sizes. For exact
  per-engine final footprint, run one engine per `kvbenchrun` invocation with
  `-keep-data`.
- The compared engines do not have identical durability contracts under the
  current kvbench configuration.

## Next Benchmark Harness Work

- Add a prebuild/read mode for large `Get` and `Scan`: build each engine's
  database once, then run read loops against that database. This avoids Go
  benchmark calibration repeatedly rebuilding 10GB+ stores.
- For external-memory tier, keep the main set to `minweight`, `pebble`,
  `goleveldb`, and `badger`; keep bbolt only when scan/space comparison matters;
  skip buntdb for large-tier runs.
- On Linux, rerun the same matrix so the anonymous-memory limit can be enforced
  instead of only reporting RSS.
