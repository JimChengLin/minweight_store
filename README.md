# minweight_store

`minweight_store` is a small single-node ordered KV store. The current storage
behavior is summarized below.

Current V0 is an in-memory ordered KV store backed by
[`minpatricia`](https://github.com/JimChengLin/minpatricia).

## V0 API

```go
store := minweight_store.New()

_ = store.Put([]byte("alpha"), []byte("one"))
value, ok, err := store.Get([]byte("alpha"))
deleted, err := store.Delete([]byte("alpha"))

item, ok, err := store.SeekGE([]byte("a"))
item, ok, err = store.SeekLE([]byte("z"))

err = store.Scan(func(item minweight_store.Item) bool {
	return true
})
err = store.ScanRange([]byte("a"), []byte("z"), func(item minweight_store.Item) bool {
	return true
})
```

Range semantics:

- `ScanRange(greaterOrEqual, lessThan)` visits `[greaterOrEqual, lessThan)`.
- `ScanRange(greaterOrEqual, nil)` visits `[greaterOrEqual, +inf)`.
- `ReverseScanRange(lessOrEqual, greaterThan)` visits `(greaterThan, lessOrEqual]`.
- `ReverseScanRange(lessOrEqual, nil)` visits `(-inf, lessOrEqual]` in descending order.
- `SeekGE` returns the first item whose key is `>= pivot`.
- `SeekLE` returns the last item whose key is `<= pivot`.
- `Delete` on a missing key returns `(false, nil)`. In WAL-backed stores it does
  not write a delete record for that miss.

## V1 mmap WAL + flush

```go
store, err := minweight_store.Open("db", minweight_store.Options{
	WALSize: 128 << 20,
})
if err != nil {
	return err
}
defer store.Close()
```

`Open` uses segmented fixed-size mmap WAL files as the record store. Index
positions are 63-bit record handles: high 33 bits are record file number, low
30 bits are offset or row inside that file. The file suffix determines the
record-store kind; current Store positions point to WAL segments under
`wal/*.wal` or compacted Parquet segments under `sst/*.parquet`.

`Flush` seals the active WAL, creates a new active WAL, syncs the new active WAL
header and WAL directory state, syncs the live primary index and sealed WAL,
writes `MANIFEST` with `primary_wal_flushed=true`, replays the sealed WAL into
the secondary checkpoint index, syncs and closes the secondary index, deletes
pending source WALs made obsolete by `install_sst`, fsyncs `wal/`, then writes
`MANIFEST` again with `primary_wal_flushed=false`. The live primary index is not
switched during flush.

`MANIFEST` stores `version`, `checkpoint_wal_file_no`, `active_wal_file_no`,
`next_file_no`, `wal_segment_size`, `primary_wal_flushed`, `seq`, and a CRC.
It is a 4KiB log of 64-byte records; normal commits append and fsync the
manifest file, and replacement is only used when the log is full. On startup, a
legal manifest with `primary_wal_flushed=false` and an empty WAL tail lets
`Open` use the primary runtime index directly: no secondary copy, no replay, and
no startup flush. If the tail is non-empty, `Open` copies the secondary
checkpoint index into the primary runtime index, replays the active WAL segment
after the checkpoint, then checkpoints that recovered state. If
`primary_wal_flushed=true`, `Open` requires an empty active WAL, trusts the
synced primary index, copies primary to secondary, and clears the flag. If
`Options.WALSize` is unset, `Open` uses manifest `wal_segment_size` for future
WAL segments; an explicit `Options.WALSize` overrides it. Existing WAL segment
files are opened at their actual file size.
With a legal manifest, startup also removes valid `sst/*.parquet.tmp` files and
uncommitted Parquet files at or above the manifest `next_file_no`, except for
Parquet files that dirty WAL replay already installed and opened.
Default options use a 128MiB WAL segment, `WALReplayPointInTime`,
`VerifyIndexOnRead=false`, `MinorCompactionThreadNum=1`,
`MajorCompactionThreadNum=1`, and `MaxImmutableWALNum=1`.
Without a manifest, the WAL directory must be empty, contain only WAL segment
1, or contain WAL segment 1 followed by an empty segment 2 left by a crashed
rollover. Startup drops that empty segment and rebuilds/syncs the primary index
by replaying WAL segment 1. The no-manifest path is WAL-only; Parquet/SST state
belongs to the manifest-backed lifecycle.

`Close` is a no-op for durability when the active WAL is empty. Otherwise it
uses the same checkpoint path as flush: rollover, sync primary/sealed WAL/new
active WAL header, publish `primary_wal_flushed=true`, replay and sync
secondary, delete pending source WALs, then publish `primary_wal_flushed=false`.

Replay policies apply when rebuilding from WAL and when replaying WAL into the
secondary checkpoint index:

- `WALReplayStrict`: any corrupt WAL record fails `Open`.
- `WALReplayPointInTime`: replay the valid prefix and truncate WAL logical used
  to the first corrupt record.
- `WALReplayBestEffort`: repair the WAL before replay by keeping CRC-valid
  records and deleting corrupt bytes, then replay the repaired WAL strictly.

Minor compaction materializes checkpointed immutable WAL records into Parquet
segments. It considers WALs with `fileNo <= checkpoint_wal_file_no` while
keeping the newest `MaxImmutableWALNum` immutable WALs. For each source WAL, it
strictly replays records, sorts put candidates by key, filters each candidate by
probing the current index position, writes live records into Parquet, syncs and
installs that segment, appends an `install_sst` WAL record (`op=3`, payload is
source WAL file number plus Parquet file number), retargets primary index
entries from the source WAL file number to the new Parquet file number, and
schedules the source WAL for deletion. Delete-only WALs can compact to an empty
Parquet segment so the source WAL deletion still has a durable `install_sst`
record. Pending source WALs are deleted only after a later checkpoint has replayed
the `install_sst` into the secondary index, and before the final
`primary_wal_flushed=false` manifest commit.
