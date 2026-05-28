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

## Engine goals

- 恢复语义清楚。
- 实现简单。
- 行为可预测。
- 性能够用即可。
- 写成功表示对后续读可见，不表示已经强同步持久化；只有 checkpoint 完成后的数据才被承诺可崩溃恢复。
- 有序访问能力属于核心语义，不能在实现或文档中降级成只支持 point `Put` / `Get` / `Delete` 的普通 KV。

## File layout

数据库目录的核心布局：

```text
db/
  MANIFEST
  index_A
  index_B
  wal/
    000000000001.wal
    000000000002.wal
  parquet/
    000000000001.parquet
    000000000002.parquet
```

- `MANIFEST` 是唯一的 durable checkpoint 提交点，记录当前可恢复状态。
- `index_A` / `index_B` 是双缓冲 mmap index 文件，一个承载 primary live index，一个承载 secondary checkpoint index。
- `wal/*.wal` 是逻辑 WAL segment，保存最近写入。
- `parquet/*.parquet` 是不可变 record segment，由 checkpoint / compaction 生成或引用。

## Manifest

- MANIFEST 是唯一的 durable checkpoint 描述：version + checkpoint WAL file no + active WAL file no + next WAL file no + WAL segment size + CRC。
- `manifestState` 只是 on-disk payload 的内存表示，不是运行时状态容器。
- Manifest 指向的是 secondary checkpoint index 的进度，不是 live primary index。
- Manifest 提交后，secondary checkpoint index 必须已经完整、干净、可恢复。
- 普通 `Put` / `Delete` 不能修改 secondary checkpoint index。
- `wal_segment_size` 表示上次 checkpoint 使用的 WAL segment 大小；`Options.WALSize` 未设置时可作为默认值，显式 option 覆盖它。
- Manifest 更新必须使用原子替换流程：

```text
write MANIFEST.tmp
fsync(MANIFEST.tmp)
rename(MANIFEST.tmp, MANIFEST)
fsync(db directory)
```

## WAL

- WAL 是逻辑日志，index 里的 record position 指向 record store 的稳定位置。
- record position 使用 63-bit 空间，当前 offset 上限是 1GiB；file number 来自 segment 文件名。
- 未来 WAL / Parquet 这类 record segment 通过文件后缀或目录语义区分，不要把 kind 编进复杂 handle 方案里。
- WAL 使用 segment 文件，不使用单文件前缀回收。
- 写路径 append WAL 后不立刻 fsync。只有 checkpoint 封存 WAL segment 并完成 sync 后，该 segment 中的数据才承诺可恢复。
- `Put` / `Delete` 成功表示对后续读可见，不表示崩溃后一定存在。
- checkpoint 之前崩溃，尾部 WAL 允许丢失。
- `ParquetSetChange` record 必须 fsync 后才能删除它移除的 Parquet 文件。

WAL record 语义：

```text
magic: u32
version: u16
header_len: u16
seq: u64
op: u8              // 1 = Put, 2 = Delete, 3 = ParquetSetChange
key_len: u32
value_len: u32      // Delete 时必须为 0
payload_len: u32
key: [u8; key_len]
value: [u8; value_len]
crc32: u32
```

- `seq` 全局单调递增。
- `payload_len == key_len + value_len`。
- Delete record 的 `value_len` 必须为 0。
- `ParquetSetChange` record 的 payload 保存 removed parquet ids 和 added parquet metadata。
- CRC 覆盖除 `crc32` 字段外的完整 record。
- replay 只接受连续、CRC 正确、seq 正确的 WAL 前缀；遇到 magic、version、长度、CRC、seq 任一错误时按当前 replay policy 处理，不跳过坏 record。

## Index and record stores

- Index 保存 `key -> location`，location 可以指向 WAL 或 Parquet。
- 单 entry 更新必须对 reader 原子可见。
- 不存在 tombstone，删除就是移除 entry。
- 支持单 writer、多 reader；reader 不会看到半写 location。
- checkpoint 把 location 从 WAL 改成 Parquet 时，逻辑 value 不变，因此允许 reader 看到新旧任一 location。
- `heapRecordStore` 是内存实现；`mmapWALRecordStore` 是 WAL-backed 实现；`parquetRecordStore` 是 Parquet-backed record segment，不要把这些概念混成一个通用 `recordStore` 名字。
- `parquetRecordStore.Sync()` 后应变成 read-only，再 append 必须报错。
- Parquet build 路径必须增量写入，build 完释放 builder 相关状态；不要 clone 后攒完整 `records []parquetRecord`。
- Parquet point read 不是热路径目标。保留受控 page cache，默认 `PageBufferSize` 为 4KiB，避免把 Parquet 做成纯内存存储。

## Runtime state and concurrency

运行时核心状态：

```text
primary_index              // live runtime index
secondary_index            // manifest checkpoint index，flush 时短暂打开
current_wal                // writable WAL segment
runtime_live_entries       // parquet_id -> live entry count
reader_epoch               // epoch/refcount manager
frozen                     // writes blocked during checkpoint/compaction
```

- 单 writer：`Put` 和 `Delete` 串行执行。
- 多 reader：point read 和有序 scan 都走 primary live index，可以并发执行。
- checkpoint 和 compaction 期间冻结写入，读请求继续服务。
- reader 使用 epoch 或 refcount 保护资源生命周期。
- 旧 WAL segment、旧 Parquet 文件、旧 index generation 只能在没有 reader 持有后删除。
- writer 只修改 `primary_index`。
- reader 只读取 `primary_index`，不读取 manifest。
- `secondary_index` 是 manifest checkpoint index，普通写入不能修改它。

## Write and read paths

`Put` 基本流程：

```text
1. 如果 frozen，阻塞等待。
2. 分配 seq。
3. append WAL Put record。
4. 更新 primary_index[key] = WAL(wal_id, offset, value_len)。
5. 如果旧 location 指向 Parquet_X，则 runtime_live_entries[X] -= 1。
6. 如果当前 WAL segment 满，触发 flush/checkpoint。
7. 返回成功。
```

`Delete` 基本流程：

```text
1. 如果 frozen，阻塞等待。
2. 分配 seq。
3. append WAL Delete record。
4. 如果 key 存在，从 primary_index 移除。
5. 如果旧 location 指向 Parquet_X，则 runtime_live_entries[X] -= 1。
6. 如果当前 WAL segment 满，触发 flush/checkpoint。
7. 返回成功。
```

- 即使 key 不存在，也可以写 Delete record。
- Delete record replay 后仍然是幂等删除。
- append WAL 成功后，如果 index 更新失败，引擎必须进入 fatal 状态并停止服务；不能返回普通错误后继续运行，否则 replay 会让一个“失败写入”复活。
- `Put` / `Delete` 遇到 WAL full 时可以循环：释放 primary lock，flush，然后重试。除 WAL full 这类可恢复边界外，不要把写入错误吞掉。

`Get` / scan 基本规则：

```text
1. 进入 reader epoch。
2. 查 primary_index。
3. 不存在则退出 epoch，返回 NotFound 或结束迭代。
4. location 是 WAL，则从对应 WAL segment 读取 value。
5. location 是 Parquet，则从对应 Parquet 读取 value。
6. 退出 reader epoch。
```

- Reader 不读取 manifest。
- Scan / range scan / reverse scan / seek 的顺序语义来自 `minpatricia`，读路径必须继续以 primary live index 为准。

## Flush and checkpoint

- runtime flush 不切换 live index，也不 promote secondary。
- flush 只需要挡住写入；读路径必须继续走 primary。实现上应拿 `primaryMu.RLock()` 和 `secondaryIndexMu`，不要用 primary 写锁包住整个 flush。
- 当前 WAL-only checkpoint 模型里，sealed WAL 满足 `old_wal.file_no == manifest.checkpoint_wal_file_no + 1`。
- Manifest 不能记录一个尚未应用 checkpoint replay 并落盘的 WAL 进度。

flush 基本顺序：

```text
1. seal / rollover 当前 active WAL。
2. 并发 sync primary index 和 sealed WAL。
3. 将 checkpoint 之后到 sealed WAL 的逻辑日志 replay 到 secondary index。
4. sync 并关闭 secondary index。
5. sync 新 active WAL header。
6. 写 manifest。
```

关键顺序不能改变：

```text
primary index 与 sealed WAL 落盘
-> secondary checkpoint replay/sync/close
-> 新 active WAL header 落盘
-> manifest 提交
```

- runtime flush / Close checkpoint replay 使用 strict policy。
- Best-effort 启动恢复要先 repair 待 replay WAL，删除坏 bytes，再进入 strict replay。
- 如果 manifest 未提交就崩溃，启动从旧 manifest checkpoint 加 WAL tail 恢复。
- 如果 manifest 已提交且 WAL tail 为空，启动直接打开 primary runtime index，不复制 secondary、不 replay、不做启动时 flush 同步。
- 如果 manifest 已提交但 WAL tail 非空，启动复制 secondary checkpoint 到 primary runtime index，再 replay manifest active WAL tail，并 sync primary。这次 replay 也算一次 flush 状态同步：继续把同一段 tail replay 到 secondary checkpoint，sync secondary，roll 到新的 active WAL，并提交新的 manifest。

## Graceful shutdown

- 如果 active WAL 为空，不做 durable work；没有新的 durable progress 要发布。
- 如果 active WAL 非空，seal active WAL，并创建新的空 active WAL；旧 active WAL 不能删除，当前 record position 仍可能指向它。
- shutdown 要 sync primary 和 sealed WAL，用 WAL replay 更新 secondary，roll/reset WAL，sync 新 active WAL header，最后写 manifest。
- 不要在 primary / secondary 之间做不必要的全量覆盖。
- Close checkpoint replay 使用 strict policy；如果启动恢复需要 best-effort repair，repair 必须发生在 strict checkpoint replay 之前。

## Recovery

- 启动恢复只信任 manifest 指向的 secondary checkpoint 进度。
- 另一个 index 不作为恢复依据，只能作为可写 index；使用它服务写入前，必须通过逻辑 replay 追平到 manifest 状态。
- 对磁盘结构缺失或 manifest 不合法要 fail fast。不要创建空 index 来掩盖缺失的 primary / secondary checkpoint。

启动状态规则：

- legal manifest + 无 WAL tail：启动时直接打开 primary index，不 replay。
- legal manifest + 有 WAL tail：从 secondary copy 到 primary，replay tail 到 primary，然后完成一次 checkpoint。
- 无 legal manifest：WAL 目录只能为空、只有 WAL segment 1，或 WAL segment 1 后跟一个空的 segment 2。空 segment 2 是 manifest 提交前 rollover 崩溃留下的，启动时删除它并从 WAL segment 1 rebuild。primary 只能视为 stale runtime state，不要静默修复缺失或损坏的 primary。

replay 规则：

- Put：更新 `primary_index[key] = WAL(wal_id, offset, value_len)`。
- Delete：移除 `primary_index[key]`。
- ParquetSetChange：从 primary_index 删除 removed parquets 的 entry，扫描 added parquets，插入 Parquet location，并更新运行时 Parquet 元数据。
- 如果发生了 replay/rebuild，必须 sync primary runtime index；只有这个步骤成功后，replay 后不再引用的 Parquet 才能被视为孤儿。
- 清理孤儿 Parquet 必须在 WAL replay 和 index sync 成功之后执行，并以 replay 后的 Parquet 集合为准。
- `runtime_live_entries` 从 manifest live entries 开始，replay WAL 期间按覆盖/删除补 delta；debug 模式下可全量扫 primary_index 校验。

## Compaction

- Compaction 用于清理低活率 Parquet 文件。
- compaction 开始前 WAL 必须为空；如果 WAL 非空，先强制 checkpoint。
- compaction 期间冻结写入，读继续服务。
- Compaction 不需要特殊 index replay 文件；它把 Parquet 集合变化写成一条 `ParquetSetChange` WAL record。

compaction 基本顺序：

```text
1. 如果 current WAL 非空，先强制 checkpoint。
2. frozen = true。
3. 选择 Parquet_X。
4. 扫 primary_index，找出所有仍指向 Parquet_X 的 key。
5. 读取这些 key 的 value，写入 Parquet_Y。
6. fsync(Parquet_Y) 并 fsync parquet directory。
7. append ParquetSetChange WAL record，fsync current_wal。
8. 在 checkpoint index 上应用 ParquetSetChange 并 sync。
9. 更新 runtime_live_entries。
10. frozen = false。
11. 等待 reader epoch 结束后删除 Parquet_X。
```

- `ParquetSetChange` record 已经 fsync 后，旧 Parquet 才能删除。
- 如果删除前崩溃，启动 replay 这条 WAL 后再清理旧 Parquet。
- 如果删除后崩溃，启动 replay 这条 WAL 后不会再引用旧 Parquet。

## Failure behavior

| 崩溃位置 | 恢复行为 | 数据影响 |
| --- | --- | --- |
| WAL record 半写 | replay 校验失败，截断到上一条完整 record。 | 半写 record 丢失。 |
| WAL 写完但未 checkpoint | 已落盘前缀可能 replay，未落盘尾部可能丢失。 | 丢失未承诺持久化的数据。 |
| Parquet 写一半 | 不在 manifest，启动清理。 | 无已承诺数据丢失。 |
| Parquet 写完但 manifest 未提交 | 不在 manifest，启动清理；WAL 仍可 replay。 | 无已承诺数据丢失。 |
| secondary checkpoint 已改，manifest 未提交 | manifest 未提交，启动按旧 checkpoint + WAL tail 恢复；candidate 不作为恢复依据。 | 无已承诺数据丢失。 |
| manifest 已提交且 WAL tail 为空 | 启动直接打开 primary runtime index。 | 无影响。 |
| manifest 已提交且 WAL tail 非空 | 启动复制 secondary checkpoint 到 primary，再 replay WAL tail，sync primary，然后把 tail 同步到 secondary 并提交新 manifest。 | 无已承诺数据丢失。 |
| compaction 删除旧 Parquet 前崩溃 | replay ParquetSetChange 后清理旧 Parquet。 | 无影响。 |
| compaction 删除旧 Parquet 后崩溃 | replay ParquetSetChange 后不会再引用旧 Parquet。 | 无影响。 |
| manifest 损坏 | 拒绝启动。 | 需要运维恢复。 |
| manifest 指向的 index 损坏 | 拒绝启动。 | 需要运维恢复。 |

## Error handling

- 如果 record 已经接受 mutation，但 index 后续失败，Store 必须进入 fatal 状态。
- fatal 只记录第一个真正的 fatal cause；后续操作返回同一个 fatal error。
- `ErrClosed` 不是 fatal。
- 对磁盘结构缺失或 manifest 不合法要 fail fast。
- 不要创建空 index、空 manifest 或空 record store 来掩盖 durable state 缺失或损坏。

## Invariants

实现必须满足：

1. Manifest 指向的 secondary checkpoint 进度必须完整、干净、可恢复。
2. Manifest 提交时引用的 Parquet 文件必须已经完整落盘。
3. 启动清理 Parquet 必须在 WAL replay 和 index sync 成功之后执行，并以 replay 后的 Parquet 集合为准。
4. Reader 只看 `primary_index`，不看 manifest。
5. 删除 WAL、Parquet、旧 index generation 前必须等待 reader epoch 结束。
6. Compaction 开始时 WAL 必须为空。
7. `ParquetSetChange` record 必须 fsync 后才能删除旧 Parquet。
8. append WAL 成功但 index 更新失败时，引擎必须进入 fatal 状态并停止服务。
9. replay 只接受连续、CRC 正确、seq 正确的 WAL 前缀。
10. checkpoint 的提交顺序固定为：primary index 与 sealed WAL 落盘、secondary replay/sync/close、新 active WAL header 落盘、manifest 提交。
11. 普通写入不能修改 Manifest 对应的 secondary checkpoint index。
12. Scan / range scan / reverse scan / seek 必须保持有序语义，不能被恢复、flush 或 compaction 路径破坏。

## Tests

- 存储语义相关修改至少跑：
  - `env GOCACHE=/private/tmp/go-build-minweight go test -timeout=30s -count=1 ./...`
- 涉及 flush、open/recovery、fatal、并发锁语义时，再跑：
  - `env GOCACHE=/private/tmp/go-build-minweight go test -race -timeout=45s -count=1 ./...`
- 性能相关修改需要补 benchmark 或更新已有 benchmark，尤其是 mmap node store、WAL record store、Parquet record store 的随机读写和连续读路径。
