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

## V1 mmap WAL

```go
store, err := minweight_store.Open("db", minweight_store.Options{
	WALSize: 64 << 20,
})
if err != nil {
	return err
}
defer store.Close()
```

`Open` uses a fixed-size mmap WAL as the record store. Index positions point to
WAL record offsets, and startup currently resets the mmap index then replays the
WAL to rebuild it. The WAL has a file header; each record stores op, key length,
value length, CRC32, key, and value. There is no per-record magic.

V1 does not implement flush, clean manifest, checkpoint, or WAL truncation yet.
