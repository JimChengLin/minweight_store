# minweight_store 设计文档

## 1. 目标

`minweight_store` 是一个简单的单机 KV 存储引擎，只支持三个 API：

| API | 语义 |
| --- | --- |
| `Put(key, value)` | 写入或覆盖 key。成功返回表示对后续读可见，不表示已经持久化。 |
| `Get(key)` | 返回 value 或 `NotFound`。 |
| `Delete(key)` | 删除 key。成功返回表示对后续读可见，不表示已经持久化。 |

设计优先级：

1. 恢复语义清楚。
2. 实现简单。
3. 行为可预测。
4. 性能够用即可。

不追求每次写入强同步。用户写入只有在 checkpoint 完成后，才被承诺可崩溃恢复。

## 2. 并发模型

- 单 writer：`Put` 和 `Delete` 串行执行。
- 多 reader：`Get` 可以并发执行。
- checkpoint 和 compaction 期间冻结写入，读请求继续服务。
- reader 使用 epoch 或 refcount 保护资源生命周期。
- 旧 WAL segment、旧 Parquet 文件、旧 index generation 只能在没有 reader 持有后删除。

## 3. 文件布局

数据库目录结构：

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

文件职责：

| 文件 | 作用 |
| --- | --- |
| `MANIFEST` | 元数据提交点，记录当前可恢复状态。 |
| `index_A` / `index_B` | 双缓冲 mmap 索引文件。 |
| `wal/*.wal` | WAL segment，保存最近写入。 |
| `parquet/*.parquet` | 不可变数据文件，由 checkpoint 和 compaction 生成。 |

## 4. Manifest

Manifest 保存 checkpoint snapshot 元数据：

```text
version: u32
active_index: A | B
next_wal_id: u64
replay_wal_id: u64
replay_offset: u64
next_seq: u64
parquets:
  - id: u64
    total_entries: u64
    live_entries: u64
```

字段语义：

| 字段 | 语义 |
| --- | --- |
| `active_index` | 崩溃恢复时可信的 clean index。 |
| `next_wal_id` | 下一个新建 WAL segment 的 id。 |
| `replay_wal_id` | 启动恢复时开始 replay 的 WAL segment id。 |
| `replay_offset` | 启动恢复时开始 replay 的 WAL offset。 |
| `next_seq` | 下一条 WAL record 应使用的 seq。 |
| `parquets` | 当前 manifest 认可的 Parquet 文件集合。 |
| `live_entries` | 截至 manifest 提交时的持久化 live 计数，不包含之后未 checkpoint 的 WAL delta。 |

Manifest 更新必须使用原子替换流程：

```text
write MANIFEST.tmp
fsync(MANIFEST.tmp)
rename(MANIFEST.tmp, MANIFEST)
fsync(db directory)
```

规则：

- Manifest 是 checkpoint snapshot 的提交点。
- Manifest 提交时引用的 Parquet 文件必须已经完整落盘。
- Manifest 指向的 `active_index` 必须是完整、干净、可恢复的 index。
- 普通 `Put/Delete` 不能修改 Manifest 指向的 `active_index`。
- 启动时必须先 replay WAL 中的 ParquetSetChange，并完成 index sync，再清理不再被引用的 Parquet 文件。

## 5. WAL

WAL 使用 segment 文件，不使用单文件前缀回收。

### 5.1 Record 格式

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

约束：

- `seq` 全局单调递增。
- `payload_len == key_len + value_len`。
- Delete record 的 `value_len` 必须为 0。
- ParquetSetChange record 的 payload 保存 removed parquet ids 和 added parquet metadata。
- CRC 覆盖除 `crc32` 字段外的完整 record。
- replay 时遇到 magic、version、长度、CRC、seq 任一错误，立即停止并截断当前 WAL segment 到失败 offset。
- replay 不跳过坏 record。

### 5.2 持久化语义

写路径 append WAL 后不立刻 fsync。

只有 checkpoint 封存 WAL segment 并完成 `fsync/msync` 后，该 segment 中的数据才承诺可恢复。

因此：

- `Put/Delete` 成功表示对后续 `Get` 可见。
- `Put/Delete` 成功不表示崩溃后一定存在。
- checkpoint 之前崩溃，尾部 WAL 允许丢失。
- ParquetSetChange record 必须 fsync 后才能删除它移除的 Parquet 文件。

## 6. Index

Index 保存：

```text
key -> location
```

location 有两类：

```text
WAL(wal_id, offset, value_len)
Parquet(parquet_id, row_group, row, value_len)
```

Index 需要满足：

- 单 entry 更新对 reader 原子可见。
- 不存在 tombstone，删除就是移除 entry。
- 支持单 writer、多 reader。
- reader 不会看到半写 location。
- checkpoint 把 location 从 WAL 改成 Parquet 时，逻辑 value 不变，因此允许 reader 看到新旧任一 location。

## 7. 内存状态

运行时维护：

```text
clean_index: A | B          // manifest.active_index
writer_index: A | B         // 当前可写 index，通常是另一个 index
serving_index: atomic pointer to A or B
current_wal: wal segment
next_seq: u64
runtime_live_entries: map[parquet_id]u64
reader_epoch: epoch/refcount manager
frozen: bool
```

规则：

- writer 只修改 `writer_index`。
- reader 只读取 `serving_index`。
- `clean_index == manifest.active_index`，普通写入不能修改它。
- 正常情况下 `writer_index == serving_index`，并且 `writer_index != clean_index`。
- checkpoint 和 compaction 期间会冻结写入，并通过 WAL replay 或 ParquetSetChange 维护可写 index。
- 删除旧文件前必须等待 reader epoch 结束。

## 8. API 流程

### 8.1 Put

```text
Put(key, value):
  1. 如果 frozen，阻塞等待。
  2. 分配 seq。
  3. append WAL Put record。
  4. 更新 writer_index[key] = WAL(wal_id, offset, value_len)。
  5. 如果旧 location 指向 Parquet_X，则 runtime_live_entries[X] -= 1。
  6. 如果当前 WAL segment 满，触发 checkpoint。
  7. 返回成功。
```

约束：

- append WAL 成功后，如果 index 更新失败，引擎必须进入 fatal 状态并停止服务。
- 不能返回普通错误后继续运行，否则 replay 会让一个“失败写入”复活。

### 8.2 Delete

```text
Delete(key):
  1. 如果 frozen，阻塞等待。
  2. 分配 seq。
  3. append WAL Delete record。
  4. 如果 key 存在，从 writer_index 移除。
  5. 如果旧 location 指向 Parquet_X，则 runtime_live_entries[X] -= 1。
  6. 如果当前 WAL segment 满，触发 checkpoint。
  7. 返回成功。
```

规则：

- 即使 key 不存在，也可以写 Delete record。
- Delete record replay 后仍然是幂等删除。

### 8.3 Get

```text
Get(key):
  1. 进入 reader epoch。
  2. idx := atomic_load(serving_index)。
  3. 查 idx。
  4. 不存在则退出 epoch，返回 NotFound。
  5. location 是 WAL，则从对应 WAL segment 读取 value。
  6. location 是 Parquet，则从对应 Parquet 读取 value。
  7. 退出 reader epoch。
```

Reader 不读取 manifest。

## 9. Checkpoint

Checkpoint 触发条件：

- 当前 WAL segment 达到大小阈值。
- compaction 前发现 WAL 非空。
- 未来如果提供显式 `Checkpoint()` API，也使用同一流程。

设当前 `clean_index = A`，`writer_index = serving_index = B`。
`A` 是 manifest 当前信任的 clean index，普通写入不能修改它。
`B` 是可写 index，已经包含 `A + 当前 WAL replay`。

正式流程：

```text
1. frozen = true，阻塞 Put/Delete，Get 继续执行。

2. 封存 current_wal：
   fsync/msync current_wal
   fsync(wal directory)
   sealed_wal_id = current_wal.id
   sealed_end = current_wal.end_offset

3. 扫描 sealed WAL 从 replay_offset 到 sealed_end：
   同 key 只保留最后一次操作。
   最后是 Delete 的 key 不写入 Parquet。
   ParquetSetChange 更新 checkpoint 目标 Parquet 集合。

4. 生成 Parquet_new：
   写入所有最后状态为 Put 的 key/value。
   fsync(Parquet_new)
   fsync(parquet directory)

5. 在 B 上应用 checkpoint replay：
   replay 材料是 sealed WAL 的最终状态和 Parquet_new 的 key/position。
   对物化到 Parquet_new 的 key：
     B[key] = Parquet(Parquet_new, position)
   对最后状态为 Delete 的 key：
     B.remove(key)
   msync(B)

6. 准备并提交新 manifest：
   active_index = B
   next_wal_id = sealed_wal_id + 1
   replay_wal_id = sealed_wal_id + 1
   replay_offset = 0
   next_seq = 当前 next_seq
   parquets = 扫描 sealed WAL 后得到的目标 Parquet 集合，并加入 Parquet_new
   parquets 更新 live_entries = runtime_live_entries
   write tmp -> fsync tmp -> rename -> fsync db directory

7. 重建 A：
   从旧 clean state 开始 replay sealed WAL。
   Put 最终状态改写为 Parquet_new location。
   Delete 最终状态从 A 移除。
   ParquetSetChange 按 WAL record 应用。
   msync(A)

8. atomic_store(serving_index, A)
   writer_index = A
   clean_index = B

9. 创建新的 current_wal，id = sealed_wal_id + 1。

10. frozen = false。

11. 等待旧 reader epoch 结束后，删除 sealed_wal 以及更老且不再需要的 WAL segment。
```

关键顺序不能改变：

```text
数据文件落盘 -> 新 primary index 落盘 -> manifest 切换 active_index -> 重建另一个 index -> serving_index 切换
```

Manifest 不能指向一个尚未应用 checkpoint replay 并落盘的 index。

如果 manifest 已提交但 A 还没有完成重建就崩溃，启动恢复仍然信任 `active_index = B`，
然后从 `active_index = B` 逻辑重建 A，再 replay WAL tail。

## 10. Compaction

Compaction 用于清理低活率 Parquet 文件。

触发条件示例：

```text
live_entries / total_entries < 0.5
```

Compaction 不需要特殊 index replay 文件。
它把 Parquet 集合变化写成一条 ParquetSetChange WAL record。

规则：

- compaction 开始前 WAL 必须为空。
- 如果 WAL 非空，先强制 checkpoint。
- compaction 期间冻结写入，读继续服务。

设当前 `writer_index = serving_index = B`。

正式流程：

```text
1. 如果 current WAL 非空，先强制 checkpoint。
   checkpoint 完成后，WAL 必须为空。

2. frozen = true。

3. 选择 Parquet_X。

4. 扫 writer_index，找出所有仍指向 Parquet_X 的 key。

5. 读取这些 key 的 value，写入 Parquet_Y。
   fsync(Parquet_Y)
   fsync(parquet directory)

6. append ParquetSetChange WAL record：
   removed = [Parquet_X]
   added = [Parquet_Y metadata]
   fsync current_wal

7. 在 B 上应用 ParquetSetChange：
   删除所有指向 Parquet_X 的 entry。
   扫描 Parquet_Y，把其中的 key 插入为 Parquet_Y location。
   msync(B)

8. 更新 runtime_live_entries：
   移除 X，加入 Y。

9. frozen = false。

10. 等待 reader epoch 结束后删除 Parquet_X。
```

ParquetSetChange record 已经 fsync 后，旧 Parquet 才能删除。
如果删除前崩溃，启动 replay 这条 WAL 后再清理旧 Parquet。
如果删除后崩溃，启动 replay 这条 WAL 后不会再引用旧 Parquet。

## 11. 启动恢复

启动流程：

```text
1. 读取并校验 MANIFEST。
   失败则拒绝启动。

2. 找 clean_index = manifest.active_index。
   找 writable_index = 另一个 index。

3. 重建 writable_index：
   清空 writable_index。
   遍历 clean_index 的 key/location，逻辑插入 writable_index。
   msync(writable_index)

4. 从 manifest.replay_wal_id / replay_offset 开始 replay WAL：
   对每条 record：
     校验 magic/version/len/CRC/seq。
     失败则截断当前 WAL 到失败 offset，停止 replay。
     Put：更新 writable_index[key] = WAL(wal_id, offset, value_len)
     Delete：移除 writable_index[key]
     ParquetSetChange：
       从 writable_index 删除 removed parquets 的 entry
       扫 added parquets，插入 Parquet location
       更新运行时 Parquet 元数据

5. msync(writable_index)。
   只有这个步骤成功后，replay 后不再引用的 Parquet 才能被视为孤儿。

6. 清理孤儿 Parquet：
   删除所有不在 replay 后运行时 Parquet 集合中的 parquet 文件。

7. 重建 runtime_live_entries：
   从 manifest.live_entries 开始。
   replay WAL 期间按覆盖/删除补 delta。
   debug 模式下可全量扫 writable_index 校验。

8. writer_index = writable_index。
   serving_index = writable_index。
   current_wal = 最后一个可追加 WAL segment。
   frozen = false。
   开服。
```

启动恢复只信任 manifest 指向的 clean index。

另一个 index 不作为恢复依据，只能作为可写 index。
使用它服务写入前，必须通过逻辑 replay 追平到 manifest 状态。

## 12. 故障行为

| 崩溃位置 | 恢复行为 | 数据影响 |
| --- | --- | --- |
| WAL record 半写 | replay 校验失败，截断到上一条完整 record。 | 半写 record 丢失。 |
| WAL 写完但未 checkpoint | 已落盘前缀可能 replay，未落盘尾部可能丢失。 | 丢失未承诺持久化的数据。 |
| Parquet 写一半 | 不在 manifest，启动清理。 | 无已承诺数据丢失。 |
| Parquet 写完但 manifest 未提交 | 不在 manifest，启动清理；WAL 仍可 replay。 | 无已承诺数据丢失。 |
| 新 clean candidate 已改，manifest 未提交 | manifest 未提交，启动按旧 clean index + WAL replay 恢复；candidate 不作为恢复依据。 | 无已承诺数据丢失。 |
| manifest 已提交，另一个 index 未重建 | 启动信任 manifest 指向的新 clean index，从 clean index + WAL replay 重建另一个 index。 | 无已承诺数据丢失。 |
| manifest 已提交，serving 未切换 | manifest 指向的新 index 已完整落盘，启动正常使用新 index。 | 无已承诺数据丢失。 |
| serving 已切换，旧 WAL 未删除 | 启动按 manifest 从新 replay 起点开始，旧 WAL 可清理。 | 无影响。 |
| compaction 删除旧 Parquet 前崩溃 | replay ParquetSetChange 后清理旧 Parquet。 | 无影响。 |
| compaction 删除旧 Parquet 后崩溃 | replay ParquetSetChange 后不会再引用旧 Parquet。 | 无影响。 |
| manifest 损坏 | 拒绝启动。 | 需要运维恢复。 |
| manifest 指向的 index 损坏 | 拒绝启动。 | 需要运维恢复。 |

## 13. 不变式

实现必须满足：

1. Manifest 指向的 `active_index` 必须完整、干净、可恢复。
2. Manifest 提交时引用的 Parquet 文件必须已经完整落盘。
3. 启动清理 Parquet 必须在 WAL replay 和 index sync 成功之后执行，并以 replay 后的 Parquet 集合为准。
4. Reader 只看 `serving_index`，不看 manifest。
5. 删除 WAL、Parquet、旧 index generation 前必须等待 reader epoch 结束。
6. Compaction 开始时 WAL 必须为空。
7. ParquetSetChange record 必须 fsync 后才能删除旧 Parquet。
8. append WAL 成功但 index 更新失败时，引擎必须进入 fatal 状态并停止服务。
9. replay 只接受连续、CRC 正确、seq 正确的 WAL 前缀。
10. checkpoint 的提交顺序固定为：数据文件落盘、新 primary index 落盘、manifest 切换 active_index、重建另一个 index、serving 切换。
11. 普通写入不能修改 Manifest 指向的 `active_index`。

## 14. 不支持的能力

当前版本不支持：

- 多 writer。
- 事务。
- MVCC。
- snapshot read。
- 范围查询。
- TTL。
- 在线 compaction。
- 每次写入强同步。
- 跨节点复制。

## 15. 关键取舍

| 取舍 | 选择 | 原因 |
| --- | --- | --- |
| 写入模型 | 单 writer | 降低索引并发复杂度。 |
| 读写并发 | checkpoint/compaction 冻结写，读继续 | 简单且读路径稳定。 |
| WAL | segment | 避免单文件前缀回收导致 offset 语义复杂。 |
| Index | A/B 双文件 | checkpoint 切换 primary index，另一个 index 可从 snapshot + WAL 重建。 |
| A/B 追平 | 从 clean index + WAL replay 重建 | 避免 page-level 全量复制，也不需要额外 index delta。 |
| Compaction | WAL 记录 ParquetSetChange | 用已有 replay 机制表达 `del k files, add j files`。 |
| 删除旧文件 | reader epoch 后延迟删除 | 防止正在执行的 reader 访问已删除资源。 |
| 持久性 | checkpoint 边界 | 明确写成功不等于已持久化。 |
