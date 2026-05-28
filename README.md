# minweight_store

`minweight_store` is a small single-node KV store evolving toward the design in
`DESIGN.md`.

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

## V1 mmap WAL + flush

```go
store, err := minweight_store.Open("db", minweight_store.Options{
	WALSize:         64 << 20,
	WALReplayPolicy: minweight_store.WALReplayStrict,
})
if err != nil {
	return err
}
defer store.Close()
```

`Open` uses segmented fixed-size mmap WAL files as the record store. Index
positions are 63-bit record handles: high 33 bits are WAL file number, low 30
bits are offset inside that file. The file suffix determines the record-store
kind; current WAL segments live under `wal/*.wal`.

`Flush` seals the active WAL, creates a new active WAL, syncs the live primary
index and sealed WAL, replays the sealed WAL into the secondary checkpoint
index, syncs and closes the secondary index, then atomically writes `MANIFEST`.
The live primary index is not switched during flush.

`MANIFEST` stores `version`, `checkpoint_wal_file_no`, `active_wal_file_no`,
`next_wal_file_no`, `wal_segment_size`, and a CRC. On startup, a legal manifest
with an empty WAL tail lets `Open` use the primary runtime index directly:
no secondary copy, no replay, and no startup flush. If the tail is non-empty,
`Open` copies the secondary checkpoint index into the primary runtime index,
replays the active WAL segment after the checkpoint, then syncs the recovered
primary index. If `Options.WALSize` is unset, `Open` uses manifest
`wal_segment_size` for future WAL segments; an explicit `Options.WALSize`
overrides it. Existing WAL segment files are opened at their actual file size.
When the tail is non-empty, startup also replays it into the secondary
checkpoint index, syncs secondary, rolls to a new active WAL, and updates
`MANIFEST`.
Without a manifest, the WAL directory must be empty, contain only WAL segment
1, or contain WAL segment 1 followed by an empty segment 2 left by a crashed
rollover. Startup drops that empty segment and rebuilds/syncs the primary index
by replaying WAL segment 1.

`Close` is a no-op for durability when the active WAL is empty. Otherwise it
performs a graceful checkpoint by syncing the live primary index and sealed WAL,
replaying new WAL records into the secondary checkpoint index, rolling to a new
empty active WAL, syncing that WAL header, then writing `MANIFEST`.

Replay policies apply when rebuilding from WAL and when replaying WAL into the
secondary checkpoint index:

- `WALReplayStrict`: any corrupt WAL record fails `Open`.
- `WALReplayPointInTime`: replay the valid prefix and truncate WAL logical used
  to the first corrupt record.
- `WALReplayBestEffort`: repair the WAL before replay by keeping CRC-valid
  records and deleting corrupt bytes, then replay the repaired WAL strictly.

Current flush still keeps records in WAL segments; Parquet materialization and
WAL garbage collection are intentionally left for later versions.
