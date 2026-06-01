# KVBench

This is an independent Go benchmark module for comparing `minweight_store`
with pure-Go embedded key/value engines. It is inspired by
`smallnest/kvbench`, but it uses Go benchmarks directly instead of a Redis
server layer so the benchmark focuses on storage engine behavior.

Run from this directory:

```sh
go test -bench . -benchmem
```

Useful narrower runs:

```sh
go test -bench 'BenchmarkLoad/' -benchmem -benchtime=1x
go test -bench 'BenchmarkOverwrite/' -benchmem
go test -bench 'BenchmarkGet/' -benchmem
go test -bench 'BenchmarkScan/' -benchmem
go test -bench 'BenchmarkSeekGE/' -benchmem
```

To record resource usage while a benchmark runs, use the runner:

```sh
go run ./cmd/kvbenchrun \
  -bench . \
  -benchtime=3s \
  -count=3 \
  -gomaxprocs=4 \
  -max-rss=10GiB \
  -max-data=100GB \
  -sample-rate=1s \
  -out results/run-$(date +%Y%m%d-%H%M%S)
```

The runner builds the benchmark test binary and writes:

- `bench.txt`: raw Go benchmark output
- `resource_samples.csv`: periodic samples
- `iostat.txt`: raw device-level `iostat` output
- `resource_summary.json`: wall time, user/system CPU time, average CPU percent, max RSS, block IO counters, peak data directory size, and maximum observed data growth rate

If the host allows `ps`, samples include process `%CPU` and RSS. RSS is
reported but is not used as a hard memory limit because file-backed mmap pages
are counted in RSS. On Linux, the runner also samples `/proc/<pid>/smaps_rollup`
`Anonymous` bytes, and `-max-rss` applies to that non-file-backed memory value.
On platforms where anonymous memory is not exposed, the memory limit is not
enforced. The summary always includes child-process `getrusage` data after the
benchmark exits.
Directory size sampling is logical data footprint, not physical device write
amplification. On platforms where per-process disk byte counters are not
available from pure Go, `block_input_ops` / `block_output_ops` are reported as
the OS `getrusage` block operation counters.

`iostat` sampling is enabled by default and can be disabled with
`-iostat=false`. It is device-level state for the whole machine, not
per-process attribution, so long benchmark runs should avoid other heavy disk
workloads. The summary reports parsed MB/s when the local `iostat` format is
recognized; read/write MB/s are only populated when the platform output exposes
that split. The raw `iostat.txt` is kept for audit.

Large-dataset benchmarks are available as `BenchmarkLargeLoad`,
`BenchmarkLargeGet`, and `BenchmarkLargeScan`. They generate keys and values on
the fly instead of keeping the whole dataset in memory. Configure them through
runner flags:

```sh
go run ./cmd/kvbenchrun \
  -bench '^BenchmarkLargeLoad/minweight$' \
  -benchtime=1x \
  -count=1 \
  -entries=3000000 \
  -value-size=4096 \
  -gomaxprocs=4 \
  -max-rss=10GiB \
  -max-data=100GB \
  -sample-rate=1s \
  -out results/large-minweight-load
```

The memory and data limits are soft runner limits: the runner samples the child
process and data directory, kills the child if an enforceable limit is exceeded,
and records the exceeded limit in `resource_summary.json`.
When the runner is used, benchmark data directories are kept until the child
benchmark process exits so `final_data_bytes` describes the completed run. If
`-keep-data` is not set, the runner removes the whole `data/` directory after
writing the summary.

For `minweight_store` tuning runs, the runner can pass selected engine options:

```sh
go run ./cmd/kvbenchrun \
  -bench '^BenchmarkLargeLoad/minweight$' \
  -benchtime=1x \
  -count=1 \
  -entries=3000000 \
  -value-size=4096 \
  -gomaxprocs=4 \
  -minweight-wal-size=256MiB \
  -minweight-max-immutable-wals=16 \
  -minweight-target-sst-size=512MiB \
  -out results/large-minweight-tuned
```

The benchmark intentionally skips engines that need external system
dependencies or cgo setup, such as RocksDB/LMDB bindings. The current set is:

- `minweight_store`
- `badger`
- `bbolt`
- `buntdb`
- `goleveldb`
- `pebble`
- `map` as an in-memory point-operation baseline

Default benchmark data follows the shape used by `smallnest/kvbench`: 9-byte
keys and 256-byte values. `BenchmarkLoad` writes a fixed 100k new keys per
iteration and should usually be run with `-benchtime=1x` for a single pure load.
`BenchmarkOverwrite` preloads the same 100k-key dataset, then measures only
updates to existing keys. Point reads and mixed workloads use the same fixed
100k-key dataset. Ordered benchmarks are only run for engines that expose
ordered iteration/seek semantics.
