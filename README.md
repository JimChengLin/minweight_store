# minweight_store

`minweight_store` is a small single-node KV store evolving toward the design in
`DESIGN.md`.

Current V0 is an in-memory ordered KV store backed by
[`minpatricia`](https://github.com/JimChengLin/minpatricia).

## V0 API

```go
store := minweight.New()

_ = store.Put([]byte("alpha"), []byte("one"))
value, ok, err := store.Get([]byte("alpha"))
deleted, err := store.Delete([]byte("alpha"))

item, ok, err := store.SeekGE([]byte("a"))
item, ok, err = store.SeekLE([]byte("z"))

err = store.Scan(func(item minweight.Item) bool {
	return true
})
err = store.ScanRange([]byte("a"), []byte("z"), func(item minweight.Item) bool {
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
