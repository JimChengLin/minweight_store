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
  -sample-rate=1s \
  -out results/run-$(date +%Y%m%d-%H%M%S)
```

The runner builds the benchmark test binary and writes:

- `bench.txt`: raw Go benchmark output
- `resource_samples.csv`: periodic samples
- `iostat.txt`: raw device-level `iostat` output
- `resource_summary.json`: wall time, user/system CPU time, average CPU percent, max RSS, block IO counters, peak data directory size, and maximum observed data growth rate

If the host allows `ps`, samples include process `%CPU` and RSS. The summary
always includes child-process `getrusage` data after the benchmark exits.
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
keys and 256-byte values. Point reads and mixed workloads use a fixed 100k-key
dataset. Ordered benchmarks are only run for engines that expose ordered
iteration/seek semantics.
