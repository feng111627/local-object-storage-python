# local-object-storage

用本地磁盘模拟对象存储后端的服务，支持**分片上传、多版本读写、配额限制、后台回收、
版本保留规则（自动淘汰）、定时自动维护与全存储总容量上限**，
在重复请求、并发冲突和异常中断下给出一致且可解释的结果。

## 快速开始

```bash
uv sync                                # 安装依赖
uv run uvicorn main:app --port 8000    # 启动服务（默认开启后台自动维护）
uv run pytest                          # 运行全部测试（85 个用例，日志含覆盖的业务点与各用例结果）
```

环境变量（均可选）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OBJECT_STORE_ROOT` | `./data` | 数据目录 |
| `OBJECT_STORE_MAX_TOTAL_SIZE` | 不限 | 整个存储的总容量上限（字节） |
| `OBJECT_STORE_MAINTENANCE_INTERVAL` | `300` | 自动维护间隔（秒） |
| `OBJECT_STORE_UPLOAD_TTL` | `3600` | 未完成分片会话的保留时长（秒） |
| `OBJECT_STORE_AUTO_MAINTENANCE` | `1` | 设为 `0` 关闭后台定时维护（仍可手动触发） |

## 对外接口契约

所有业务错误统一返回 `{"error": {"code", "message", "detail"}}`，`code` 稳定可枚举（见错误分类）。

### 对象读写

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/objects/{key}` | 整体写入，生成新版本；返回 `201 {version_id, size, sha256, ...}` |
| GET | `/objects/{key}` | 读最新可见版本；响应头带 `X-Version-Id`、`X-Object-SHA256` |
| GET | `/objects/{key}?version_id=...` | 读指定版本（含淘汰版本的可区分错误，见下） |
| DELETE | `/objects/{key}` | 整 key 删除（写入删除标记） |
| DELETE | `/objects/{key}?version_id=...` | 删除指定版本（冻结版本返回 `409 version_protected`） |
| GET | `/objects/{key}/versions` | 版本清单（最新在前，含 `deleted`/`expired`/`frozen`/`data_reclaimed` 标记） |
| POST | `/objects/{key}/versions/{vid}/freeze` | 冻结/法律保留某版本（`201`） |
| POST | `/objects/{key}/versions/{vid}/unfreeze` | 解除冻结 |

### 分片上传

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/uploads?key=...&expected_size=...&expected_sha256=...` | 创建会话，可声明整体大小/校验值用于合并时校验；返回 `201 {upload_id}` |
| PUT | `/uploads/{upload_id}/parts/{n}` | 上传分片；可选请求头 `X-Part-SHA256` 做分片级校验 |
| GET | `/uploads/{upload_id}/parts` | 查询已上传分片清单（断点续传据此跳过已完成分片） |
| POST | `/uploads/{upload_id}/complete` | 合并；body 可选 `{"parts": [1,2,...]}` 指定顺序，缺省按分片号升序 |
| DELETE | `/uploads/{upload_id}` | 中止会话并清理分片，返回 `204` |

### 版本保留规则

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/retention/rules` | 创建规则；body `{"scope":"key"\|"prefix","match":...,"max_versions":N,"max_age_seconds":T}` |
| GET | `/retention/rules` | 列出全部规则 |
| GET | `/retention/rules?key=...` | 查看某 key 实际命中的规则 |
| GET/PUT/DELETE | `/retention/rules/{id}` | 查询 / 更新（upsert）/ 删除单条规则 |

`max_versions` 与 `max_age_seconds` 至少配置一个；规则持久化在 `retention.json`，
调整即时生效、重启不丢。匹配优先级：**精确 key > 最长前缀 > 无规则**。

### 自动维护与回收

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/maintenance/run` | 立即执行一轮「淘汰 + 回收」；已有一轮在跑时返回 `409 maintenance_busy` |
| GET | `/maintenance/status` | 最近一次运行时间/结果、累计轮次、是否在跑、后台线程状态、最近错误 |
| POST | `/gc` | 只执行空间回收（不含淘汰），返回统计 |

### 容量

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/storage/usage` | `used_bytes` / `reserved_bytes` / `max_total_size` / `available_bytes` |

## 版本保留规则与淘汰语义

- **两个淘汰维度**（同一规则可同时配置，取并集）：
  - `max_versions`：每个 key 最多保留多少个**存活数据版本**，超出从最旧开始淘汰；
  - `max_age_seconds`：版本存活超过该时长即淘汰。
- **三类版本任何情况下都不会被淘汰**：
  1. 当前对外可见的最新版本；
  2. 正被读取的版本（read pin，回收会等读完再删，等不到则本轮跳过、下轮再清）；
  3. 被显式 **freeze（冻结/法律保留）** 的版本；它也拒绝显式删除（`409 version_protected`）。
  - 冻结一个"已淘汰但数据尚未回收"的版本会撤销淘汰（legal hold 优先）。
- **淘汰 ≠ 物理删除**：淘汰先打 `expired` 标记（数据保留），由自动维护/GC 再回收数据。
  淘汰是**最终一致**的：写在一轮扫描中途产生的版本会在下一轮处理。
- **错误可区分**：按版本号读取时——
  - 版本号从未存在 → `404 no_such_version`；
  - 版本被显式删除 → `410 version_already_deleted`；
  - 版本按保留规则被淘汰（无论数据是否已回收）→ `410 version_expired`。
  - 普通（不带版本号）读取会跳过淘汰版本回退到最近可见版本；整 key 删除标记仍阻断可见性、
  不会"复活"旧版本。

## 自动维护：触发时机与可观测方式

- 服务启动时自动拉起一个后台守护线程，按 `OBJECT_STORE_MAINTENANCE_INTERVAL` 周期性执行
  「保留规则淘汰 → 空间回收」，**无需人工触发**；关闭进程或设 `OBJECT_STORE_AUTO_MAINTENANCE=0`
  可停用，停用后仍可用 `POST /maintenance/run` 手动跑。
- 手动触发与定时触发共用同一把运行锁，**同一时刻最多一轮**，不重叠、不排队。
- 并发安全（不卡正常读写/分片上传）：
  - 逐 key 加锁，维护一个 key 不阻塞其它 key 的流量；
  - 读对象在定位版本后释放 key 锁、仅持有 read pin 再读文件，因此不会读到半成品，
    也不会让回收误删在读数据；
  - 数据文件一律"写临时文件 + `os.replace`"原子落盘，读者只见完整版本；
  - 维护内部异常会记录到状态并继续下一轮，不影响存储主流程。
- **可观测**（`GET /maintenance/status`，持久化在 `state/maintenance.json`，重启不丢）：
  - `total_runs` 累计完成轮次；`last_started_at` / `last_finished_at` 最近一轮起止；
  - `last_result.retention.versions_expired` 这一轮淘汰了几个版本、多少字节；
  - `last_result.gc.*` 回收的版本文件/字节/孤儿文件/过期会话/因在读而跳过的数量；
  - `running` / `scheduler_running` 当前是否有一轮在跑、后台线程是否在线；`last_error` 最近失败原因。

## 总容量上限判定规则

`QuotaConfig.max_total_size`（环境变量 `OBJECT_STORE_MAX_TOTAL_SIZE`）是**整个存储**的硬上限，
与单对象配额 `max_object_size` 等相互独立。逻辑占用 = 所有存活版本数据 + 在传分片。

- **判定点**：整体写入 PUT、每个新分片上传、分片合并（按合并完成后的**最终占用**判定）；
- 超限一律**明确拒绝**：`507 storage_capacity_exceeded`（单对象大小超限仍是 `413 object_too_large`），
  `detail` 里带 `need/used_bytes/reserved_bytes/max_total_size`，**绝不会只写入一部分当成功**；
- 容量按"预留 → 落盘 → 入账"管理，并发写入不会超额；写入失败立即回滚预留；
- 中止分片 / 回收删除数据会释放占用；**腾出空间后，之前因容量不足被拒的写入可直接重试成功**；
- 用量账本持久化在 `state/usage.json`，进程启动时会**按磁盘实际占用重建**，崩溃/中断也不会算错。

## 并发与版本化语义

- **版本不可变**：版本数据一旦落盘即不可变；覆盖写生成新版本，删除/淘汰只是打标记，
  旧版本数据由 GC 异步回收。
- **读写快照隔离**：跨 key 共享状态（容量账本）用全局短临界区锁，单 key 元数据用 per-key 锁，
  单上传会话用 per-upload 锁；唯一嵌套顺序是「上传锁 → key 锁」（合并时），无死锁。
- **删除语义**（对齐 S3 版本化）：删除指定版本 → 标记删除（冻结版本除外）；
  整 key 删除 → 写删除标记，普通读取 404，历史版本仍可按版本号读取，不会"复活"。
- **分片幂等**：重复上传同一分片内容一致 → 幂等成功；不一致 → `409 part_conflict`。
- **重复合并**：会话成功合并后即销毁，再次合并返回 `404 no_such_upload`，不会生成第二个版本。
- **断点续传**：会话与分片持久化在磁盘，进程重启后可用 `GET /uploads/{id}/parts` 确认进度继续上传。
- **合并校验**：缺片拒绝（`missing_parts`），合并后校验声明的大小/SHA-256，
  任一不通过都拒绝且不产生任何版本——不会悄悄拼出损坏的对象。

## 错误分类

| HTTP | code | 含义 |
| --- | --- | --- |
| 404 | `no_such_key` | key 不存在或已被整 key 删除 |
| 404 | `no_such_version` | 指定的版本号从未存在 |
| 410 | `version_already_deleted` | 版本存在但被显式删除（与淘汰、404 可区分） |
| 410 | `version_expired` | 版本按保留规则被淘汰（数据可能已回收） |
| 409 | `version_protected` | 版本被冻结/保留，拒绝删除等破坏性操作 |
| 404 | `no_such_upload` | 上传会话不存在（含已合并/已中止/被回收） |
| 409 | `upload_already_completed` | 会话已合并完成 |
| 409 | `part_conflict` | 同一分片被重复/并发上传且内容不一致 |
| 400 | `part_checksum_mismatch` | 分片内容校验失败 |
| 400 | `missing_parts` | 合并清单引用了未上传的分片（`detail.missing` 列出缺失分片号） |
| 400 | `complete_checksum_mismatch` | 合并结果与声明不符（`detail.reason` 区分 `size_mismatch`/`sha256_mismatch`） |
| 413 | `object_too_large` | 对象/分片/累计大小超单对象配额 |
| 413 | `too_many_parts` | 分片号超出上限 |
| 429 | `too_many_concurrent_uploads` | 同一 key 并发上传会话数超限 |
| 507 | `storage_capacity_exceeded` | 整个存储总容量不足（`detail` 带 need/used/reserved/limit） |
| 400 | `invalid_retention_rule` | 保留规则参数非法 |
| 404 | `no_such_retention_rule` | 保留规则不存在 |
| 409 | `maintenance_busy` | 已有一轮维护在执行（仅接口层返回） |

## 配额规则

配额由 `QuotaConfig` 配置（默认值见 `object_storage/models.py`）：

- `max_object_size`：单对象整体大小上限（PUT 体、声明大小、累计分片、合并结果四处都会校验）；
- `max_part_size`：单个分片大小上限；
- `max_parts_per_upload`：单次上传的分片号上限（1..N）；
- `max_concurrent_uploads`：同一 key 的活跃上传会话数上限（中止/合并/回收后释放名额）；
- `max_total_size`：整个存储的总容量上限（`None` 不限；判定规则见上节）。

所有超限都**明确拒绝**（对应上述错误码），不会静默丢弃或截断。

## 后台回收

维护/回收每一轮：

1. 按保留规则把超量/超龄版本标记为 `expired`（当前版本、在读版本、冻结版本豁免）；
2. 回收元数据已标记 `deleted` / `expired` / 删除标记的版本数据文件（元数据保留，错误语义不变）；
3. 回收无元数据引用的孤儿文件（如崩溃在"写文件"与"登记元数据"之间的残留）；
4. 中止并清理超过 `upload_ttl` 的未完成上传会话。

回收逐 key 加锁、删除前检查 read pin，**绝不触碰仍被引用或正在读取的数据**；
可重复执行（幂等），统计字段说明每类回收了多少。

## 本地验证

```bash
uv run pytest                                   # 全部 85 个用例
uv run pytest tests/test_store.py -k multipart  # 只看分片上传相关
uv run pytest tests/test_retention.py           # 保留规则/淘汰/三类保护
uv run pytest tests/test_maintenance.py         # 定时维护/可观测/并发
uv run pytest tests/test_capacity.py            # 总容量
```

`log_cli` 已开启：日志会打出每个用例**覆盖的业务点**（"业务覆盖：…"）以及开始/通过记录和耗时。
覆盖范围：

- `tests/test_store.py`：分片主流程、断点续传（含跨进程重启恢复）、幂等重传、
  并发同一分片仲裁、并发读写快照隔离、分片/整体校验失败、版本化与删除语义、配额；
- `tests/test_gc.py`：删除版本回收、孤儿文件、过期会话、存活版本保护、回收幂等；
- `tests/test_retention.py`：规则 CRUD/校验/重启持久化、key 与最长前缀匹配、
  数量与年龄淘汰、当前版本/在读版本/冻结版本三类保护、淘汰与"本来不存在"错误可区分；
- `tests/test_maintenance.py`：单轮统计、状态持久化跨重启、定时自动触发、
  手动与定时不重叠、与持续读写并发、不阻塞无关 key；
- `tests/test_capacity.py`：PUT/分片/合并三处超限拒绝、507 与 413 可区分、
  无部分成功、回收后重试成功、中止释放、全生命周期账实相符、重启重建账本；
- `tests/test_api.py` / `tests/test_api_new.py`：HTTP 契约、错误码、分片全流程、
  保留规则/冻结/维护状态/容量查询接口。

### 手工启动走一遍

```bash
# 1) 启动：1MB 总容量、2 秒一次自动维护
OBJECT_STORE_MAX_TOTAL_SIZE=1000000 \
OBJECT_STORE_MAINTENANCE_INTERVAL=2 \
uv run uvicorn main:app --port 8000

# 2) 配一条前缀保留规则（logs/ 下每个 key 最多保留 2 个版本）
curl -s -X POST localhost:8000/retention/rules -H 'content-type: application/json' \
  -d '{"scope":"prefix","match":"logs/","max_versions":2}'

# 3) 连写 4 个版本，冻结最旧版本
for i in 1 2 3 4; do curl -s -X PUT --data-binary "v$i" localhost:8000/objects/logs/a.log >/dev/null; done
OLD=$(curl -s localhost:8000/objects/logs/a.log/versions | jq -r '.versions[-1].version_id')
curl -s -X POST localhost:8000/objects/logs/a.log/versions/$OLD/freeze

# 4) 立即跑一轮维护（或等 2 秒让它自己跑）
curl -s -X POST localhost:8000/maintenance/run

# 5) 看可观测状态与版本清单
curl -s localhost:8000/maintenance/status | jq '{total_runs,last_trigger,running,scheduler_running}'
curl -s localhost:8000/objects/logs/a.log/versions \
  | jq -r '.versions[] | "\(.version_id[0:8]) expired=\(.expired) frozen=\(.frozen) reclaimed=\(.data_reclaimed)"'
# 最新版本与冻结版本仍在，中间两版 expired=true 且 data_reclaimed=true

# 6) 错误可区分：淘汰版本 410 version_expired，不存在版本 404 no_such_version
curl -i "localhost:8000/objects/logs/a.log?version_id=$OLD"   # 冻结版本 200
EXP=$(curl -s localhost:8000/objects/logs/a.log/versions | jq -r '.versions[]|select(.expired)|.version_id' | head -1)
curl -i "localhost:8000/objects/logs/a.log?version_id=$EXP"     # HTTP/1.1 410 ... version_expired
curl -i "localhost:8000/objects/logs/a.log?version_id=nope"     # HTTP/1.1 404 ... no_such_version

# 7) 容量查询；分片上传/合并超限时返回 507 storage_capacity_exceeded
curl -s localhost:8000/storage/usage | jq
```

## 目录结构

```
object_storage/
  errors.py       # 错误分类（稳定 code + HTTP 状态）
  models.py       # 数据模型、配额配置、保留规则模型
  store.py        # 核心存储：版本化读写、分片上传、per-key 锁、read pin、冻结、容量账本
  gc.py           # 空间回收（pin 感知、逐 key 加锁）
  retention.py    # 保留规则管理与淘汰判定
  maintenance.py  # 定时自动维护与持久化运行状态
  api.py          # FastAPI 路由
main.py           # 服务入口（环境变量装配、维护线程生命周期）
tests/            # 单元/接口测试
```
