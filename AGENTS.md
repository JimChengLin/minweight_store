# AGENTS.md

## Working agreements

- 不要过度防御编程。
- 不要加无意义的类型转换。
- 不要随意删除注释。
- 尽量使用中文回答。
- 新建 git branch 名字不要包含 codex。
- 禁止给出模糊的说法。
- 偏好 fail fast，而不是掩盖问题的 fallback。
- 已经添加到 git staged changes 的文件不要随意移出。

## Project conventions

- 包名保持 `minweight_store`。
- 这个项目不是普通 KV wrapper。`minpatricia` 的有序能力是核心能力，`Scan` / range scan / reverse scan / seek 语义必须保持一等支持。
- 命名必须反映真实职责和存储介质：
  - heap 实现叫 `heapNodeStore` / `heapRecordStore`。
  - mmap WAL 实现叫 `mmapWALRecordStore`。
  - 多 segment 聚合叫 `segmentedRecordStore`，不要用 `diskStore`、`recordStoreHandle` 这类含混名字。
  - primary / secondary index 的语义不能混用。primary 是 live index；secondary 是 checkpoint index。
- 如果一个字段只是从已有组件可推导出来，优先不要重复存。例如目录从 manifest path 推导，配置只在 open 时解析。

## Durability model

- WAL 是逻辑日志，index 里的 record position 指向 record store 的稳定位置。
- record position 使用 63-bit 空间，当前 offset 上限是 1GiB；file number 来自 segment 文件名。未来 WAL / Parquet 这类 record segment 通过文件后缀或目录语义区分，不要把 kind 编进复杂 handle 方案里。
- MANIFEST 是唯一的 durable checkpoint 描述：version + checkpoint WAL file no + active WAL file no + next WAL file no + WAL segment size + CRC。`manifestState` 只是 on-disk payload 的内存表示，不是运行时状态容器。
- legal manifest + 无 WAL tail：启动时直接打开 primary index，不 replay。
- legal manifest + 有 WAL tail：从 secondary copy 到 primary，replay tail 到 primary，然后完成一次 checkpoint。
- 无 legal manifest：WAL 目录只能为空、只有 WAL segment 1，或 WAL segment 1 后跟一个空的 segment 2。空 segment 2 是 manifest 提交前 rollover 崩溃留下的，启动时删除它并从 WAL segment 1 rebuild。primary 只能视为 stale runtime state，不要静默修复缺失或损坏的 primary。
- graceful shutdown：如果 active WAL 为空，不做 durable work；否则 sync primary 和 sealed WAL，用 WAL replay 更新 secondary，roll/reset WAL，sync 新 active WAL header，最后写 manifest。不要在 primary/secondary 之间做不必要的全量覆盖。

## Flush rules

- runtime flush 不切换 live index，也不 promote secondary。
- flush 只需要挡住写入；读路径必须继续走 primary。实现上应拿 `primaryMu.RLock()` 和 `secondaryIndexMu`，不要用 primary 写锁包住整个 flush。
- flush 基本顺序：
  1. seal / rollover 当前 active WAL。
  2. 并发 sync primary index 和 sealed WAL。
  3. 将 checkpoint 之后到 sealed WAL 的逻辑日志 replay 到 secondary index。
  4. sync 并关闭 secondary index。
  5. sync 新 active WAL header。
  6. 写 manifest。
- runtime flush / Close checkpoint replay 使用 strict policy。Best-effort 启动恢复要先 repair 待 replay WAL，删除坏 bytes，再进入 strict replay。
- `Put` / `Delete` 遇到 WAL full 时可以循环：释放 primary lock，flush，然后重试。除 WAL full 这类可恢复边界外，不要把写入错误吞掉。

## Error handling

- 如果 record 已经接受 mutation，但 index 后续失败，Store 必须进入 fatal 状态。
- fatal 只记录第一个真正的 fatal cause；后续操作返回同一个 fatal error。
- `ErrClosed` 不是 fatal。
- 对磁盘结构缺失或 manifest 不合法要 fail fast。不要创建空 index 来掩盖缺失的 primary / secondary checkpoint。

## Record stores

- `heapRecordStore` 是内存实现；`mmapWALRecordStore` 是 WAL-backed 实现；`parquetRecordStore` 是 Parquet-backed record segment，不要把这些概念混成一个通用 `recordStore` 名字。
- Parquet build 路径必须增量写入，build 完释放 builder 相关状态；不要 clone 后攒完整 `records []parquetRecord`。
- `parquetRecordStore.Sync()` 后应变成 read-only，再 append 必须报错。
- Parquet point read 不是热路径目标。保留受控 page cache，默认 `PageBufferSize` 为 4KiB，避免把 Parquet 做成纯内存存储。

## Tests

- 存储语义相关修改至少跑：
  - `env GOCACHE=/private/tmp/go-build-minweight go test -timeout=30s -count=1 ./...`
- 涉及 flush、open/recovery、fatal、并发锁语义时，再跑：
  - `env GOCACHE=/private/tmp/go-build-minweight go test -race -timeout=45s -count=1 ./...`
- 性能相关修改需要补 benchmark 或更新已有 benchmark，尤其是 mmap node store、WAL record store、Parquet record store 的随机读写和连续读路径。
