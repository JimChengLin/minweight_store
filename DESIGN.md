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
checkpoint_wal_file_no: u64
active_wal_file_no: u64
next_wal_file_no: u64
wal_segment_size: u64
crc: u32
```

字段语义：

| 字段 | 语义 |
| --- | --- |
| `checkpoint_wal_file_no` | secondary checkpoint index 已经完整应用并落盘的最后一个 WAL segment。 |
| `active_wal_file_no` | 当前可写 WAL segment。 |
| `next_wal_file_no` | 下一个新建 WAL segment 的 file number。 |
| `wal_segment_size` | 上次 checkpoint 使用的 WAL segment 大小；`Options.WALSize` 未设置时作为默认值，显式 option 会覆盖它。 |
| `crc` | Manifest 固定字段的 CRC32。 |

Manifest 更新必须使用原子替换流程：

```text
write MANIFEST.tmp
fsync(MANIFEST.tmp)
rename(MANIFEST.tmp, MANIFEST)
fsync(db directory)
```

规则：

- Manifest 是 checkpoint snapshot 的提交点。
- Manifest 指向的是 secondary checkpoint 的进度，不是 live primary index。
- Manifest 提交后，secondary checkpoint index 必须已经完整、干净、可恢复。
- 普通 `Put/Delete` 不能修改 secondary checkpoint index。
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
primary_index              // live runtime index
secondary_index            // manifest checkpoint index，flush 时短暂打开
current_wal: wal segment
runtime_live_entries: map[parquet_id]u64
reader_epoch: epoch/refcount manager
frozen: bool
```

规则：

- writer 只修改 `primary_index`。
- reader 只读取 `primary_index`，不读取 manifest。
- `secondary_index` 是 manifest checkpoint index，普通写入不能修改它。
- checkpoint/flush 期间冻结写入，并通过 WAL replay 维护 `secondary_index`。
- 删除旧文件前必须等待 reader epoch 结束。

## 8. API 流程

### 8.1 Put

```text
Put(key, value):
  1. 如果 frozen，阻塞等待。
  2. 分配 seq。
  3. append WAL Put record。
  4. 更新 primary_index[key] = WAL(wal_id, offset, value_len)。
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
  4. 如果 key 存在，从 primary_index 移除。
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
  2. 查 primary_index。
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

当前实现先不把 sealed WAL 编译成 Parquet，也不切换 live index。
运行时固定有两个 index：

- `primary`：live index，普通读写都访问它。
- `secondary`：checkpoint index，只在 checkpoint/flush 期间打开、replay、sync、close。

`MANIFEST` 记录 secondary checkpoint 已经覆盖到哪个 WAL segment。

正式流程：

```text
1. frozen = true，阻塞 Put/Delete，Get 继续执行。

2. old_wal = current_wal。

3. seal old_wal，并创建新的 current_wal。

4. 并发执行：
   msync(primary index)
   msync(old_wal)

5. 打开 secondary checkpoint index。

6. replay old_wal。当前 WAL-only checkpoint 模型里，
   `old_wal.file_no == manifest.checkpoint_wal_file_no + 1`。

7. msync(secondary index)。

8. close(secondary index)。

9. msync(new current_wal header)，确保 manifest 引用的新 active WAL 已经存在。

10. 原子提交 MANIFEST：
    checkpoint_wal_file_no = old_wal.file_no
    active_wal_file_no = current_wal.file_no
    next_wal_file_no = current_wal.file_no + 1
    wal_segment_size = 当前 WAL segment size

11. frozen = false。
```

关键顺序不能改变：

```text
primary index 与 sealed WAL 落盘 -> secondary checkpoint replay/sync/close -> 新 active WAL header 落盘 -> manifest 提交
```

Manifest 不能记录一个尚未应用 checkpoint replay 并落盘的 WAL 进度。

如果 manifest 未提交就崩溃，启动从旧 manifest checkpoint 加 WAL tail 恢复。
如果 manifest 已提交且 WAL tail 为空，启动直接打开 primary runtime index，
不复制 secondary、不 replay、不做启动时 flush 同步。
如果 manifest 已提交但 WAL tail 非空，启动复制 secondary checkpoint 到
primary runtime index，再 replay manifest active WAL tail，
并 sync primary。这次 replay 也算一次 flush 状态同步：继续把同一段 tail
replay 到 secondary checkpoint，sync secondary，roll 到新的 active WAL，
并提交新的 manifest。

graceful shutdown 是单独的收尾流程，不切换 primary，也不用 primary
覆盖 secondary；如果 checkpoint 前进，secondary 仍然通过 WAL replay
对齐 checkpoint：

```text
1. 拿 primary 写锁，阻塞 Put/Delete/Get。

2. 如果当前 active WAL 为空，直接结束；没有新的 durable progress 要发布。

3. 如果当前 active WAL 非空，或还没有合法 manifest：
   seal active WAL，并创建新的空 active WAL。
   旧 active WAL 不能删除；当前实现里 record position 仍可能指向它。

4. 并发执行：
   msync(primary index)
   msync(刚 seal 的 WAL)

5. 将 checkpoint 之后到刚 seal 的 WAL replay 到
   secondary checkpoint index，sync 并关闭 secondary。best-effort 启动
   恢复会在 replay 前 repair 待 replay WAL，删除坏 bytes；repair 后
   checkpoint replay 仍然使用 strict。

6. msync(new active WAL header)，确保 manifest 引用的新 active WAL 已存在。

7. 原子提交 MANIFEST。
```

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

设当前读写都走 `primary_index`。

正式流程：

```text
1. 如果 current WAL 非空，先强制 checkpoint。
   checkpoint 完成后，WAL 必须为空。

2. frozen = true。

3. 选择 Parquet_X。

4. 扫 primary_index，找出所有仍指向 Parquet_X 的 key。

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

2. 如果 MANIFEST 存在：
   检查 manifest active WAL 是否有 tail。
   如果 tail 为空，直接打开 primary runtime index。
   如果 tail 非空，复制 secondary checkpoint index 到 primary runtime index，
   再 replay WAL tail。

3. 如果 MANIFEST 不存在：
   清空 primary runtime index。
   WAL 目录只能为空、只有 WAL segment 1，或 WAL segment 1 后跟一个
   manifest 提交前 rollover 留下的空 segment 2。空 segment 2 启动时删除，
   其他多 segment 状态拒绝启动。
   从 WAL segment 1 replay。

4. replay WAL：
   对每条 record：
     校验 version/len/CRC。
     失败则截断当前 WAL 到失败 offset，停止 replay。
     Put：更新 primary_index[key] = WAL(wal_id, offset, value_len)
     Delete：移除 primary_index[key]
     ParquetSetChange：
       从 primary_index 删除 removed parquets 的 entry
       扫 added parquets，插入 Parquet location
       更新运行时 Parquet 元数据

5. 如果发生了 replay/rebuild，则 msync(primary runtime index)。
   只有这个步骤成功后，replay 后不再引用的 Parquet 才能被视为孤儿。

6. 清理孤儿 Parquet：
   删除所有不在 replay 后运行时 Parquet 集合中的 parquet 文件。

7. 重建 runtime_live_entries：
   从 manifest.live_entries 开始。
   replay WAL 期间按覆盖/删除补 delta。
   debug 模式下可全量扫 primary_index 校验。

8. primary_index = primary runtime index。
   current_wal = 最后一个可追加 WAL segment。
   frozen = false。
   开服。
```

启动恢复只信任 manifest 指向的 secondary checkpoint 进度。

另一个 index 不作为恢复依据，只能作为可写 index。
使用它服务写入前，必须通过逻辑 replay 追平到 manifest 状态。

## 12. 故障行为

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

## 13. 不变式

实现必须满足：

1. Manifest 指向的 secondary checkpoint 进度必须完整、干净、可恢复。
2. Manifest 提交时引用的 Parquet 文件必须已经完整落盘。
3. 启动清理 Parquet 必须在 WAL replay 和 index sync 成功之后执行，并以 replay 后的 Parquet 集合为准。
4. Reader 只看 `primary_index`，不看 manifest。
5. 删除 WAL、Parquet、旧 index generation 前必须等待 reader epoch 结束。
6. Compaction 开始时 WAL 必须为空。
7. ParquetSetChange record 必须 fsync 后才能删除旧 Parquet。
8. append WAL 成功但 index 更新失败时，引擎必须进入 fatal 状态并停止服务。
9. replay 只接受连续、CRC 正确、seq 正确的 WAL 前缀。
10. checkpoint 的提交顺序固定为：primary index 与 sealed WAL 落盘、secondary replay/sync/close、新 active WAL header 落盘、manifest 提交。
11. 普通写入不能修改 Manifest 对应的 secondary checkpoint index。

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
| Index | primary/secondary 双文件 | flush 不切 live index；secondary 只作为 checkpoint 产物。 |
| checkpoint 追平 | secondary 从 manifest checkpoint + WAL replay 追平 | 避免 page-level 全量复制，也不需要额外 index delta。 |
| Compaction | WAL 记录 ParquetSetChange | 用已有 replay 机制表达 `del k files, add j files`。 |
| 删除旧文件 | reader epoch 后延迟删除 | 防止正在执行的 reader 访问已删除资源。 |
| 持久性 | checkpoint 边界 | 明确写成功不等于已持久化。 |
