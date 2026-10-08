# local-object-storage

用本地磁盘模拟对象存储后端的服务，支持**分片上传、多版本读写、配额与总容量限制、
版本保留规则、自动维护（过期淘汰 + 后台回收）**，在重复请求、并发冲突和异常中断下
给出一致且可解释的结果。维护能力到点自动运行，无需人工盯守，并提供可观测状态。

## 快速开始

```bash
uv sync                                # 安装依赖
uv run uvicorn main:app --port 8000    # 启动服务（默认开启自动维护）
uv run pytest                          # 运行全部测试（日志含各用例覆盖的业务点与执行结果）
```

启动时的环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OBJECT_STORE_ROOT` | `./data` | 数据目录 |
| `OBJECT_STORE_MAX_TOTAL_BYTES` | 不限 | 整个存储的总容量上限（字节），例如 `104857600` = 100 MiB |
| `OBJECT_STORE_MAINTENANCE_INTERVAL` | `300` | 自动维护间隔（秒） |
| `PORT` | `8000` | 监听端口 |

## 对外接口契约

所有业务错误统一返回 `{"error": {"code", "message", "detail"}}`，`code` 稳定可枚举（见错误分类）。

### 对象读写

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/objects/{key}` | 整体写入，生成新版本；返回 `201 {version_id, size, sha256, ...}` |
| GET | `/objects/{key}` | 读最新可见版本；响应头带 `X-Version-Id`、`X-Object-SHA256` |
| GET | `/objects/{key}?version_id=...` | 读指定版本 |
| DELETE | `/objects/{key}` | 整 key 删除（写入删除标记） |
| DELETE | `/objects/{key}?version_id=...` | 删除指定版本 |
| GET | `/objects/{key}/versions` | 版本清单（最新在前，含删除标记） |
| PUT | `/objects/{key}/versions/{version_id}/hold` | 冻结版本（任何回收/淘汰/删除都不触碰） |
| DELETE | `/objects/{key}/versions/{version_id}/hold` | 解冻版本 |

### 保留规则

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/retention` | 列出全部规则 |
| GET | `/retention?pattern=<key>` | 查询某个 key 实际命中的规则（`matched`，无命中为 null） |
| PUT | `/retention` | 新建/覆盖一条规则，body：`{"scope","pattern","max_versions","max_age_seconds"}` |
| DELETE | `/retention?scope=key&pattern=...` | 删除一条规则，`204` |

规则字段：

- `scope`：`key`（精确匹配某个 key）或 `prefix`（按前缀匹配）；
- `pattern`：key 名或前缀（前缀规则例如 `logs/`）；
- `max_versions`：最多保留多少个版本，超出部分的旧版本可被淘汰；`null` 表示不限；
- `max_age_seconds`：版本最长保留秒数，超龄版本可被淘汰；`null` 表示不限。
- 两个维度同时配置时，**任一命中即淘汰**。规则持久化在 `retention.json`，重启不丢；
  同一 `scope+pattern` 重复 PUT 为覆盖更新。匹配优先级：**精确 key > 最长前缀**。

### 自动维护与容量

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/maintenance/run` | 立即跑一轮维护（淘汰 + 回收），返回 `{ran, stats, status}` |
| GET | `/maintenance/status` | 调度器与最近运行的可观测状态 |
| GET | `/capacity` | 当前总容量判定信息 |
| POST | `/gc` | 只跑旧的回收逻辑（等价于一轮维护的回收部分，保留向后兼容） |

`GET /maintenance/status` 返回：`scheduler_running`（后台调度器是否在跑）、
`currently_running`（此刻是否有一轮在执行）、`interval_seconds`、`total_runs`（累计轮次，
持久化重启接着计数）、`last_run_at`、`last_duration_ms`、`last_stats`（上一轮清掉了什么：
淘汰版本数、回收文件数、释放字节数、过期会话数）、`last_error`（上一轮异常，正常为 null）、
`capacity`。

### 分片上传

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/uploads?key=...&expected_size=...&expected_sha256=...` | 创建会话，可声明整体大小/校验值用于合并时校验；返回 `201 {upload_id}` |
| PUT | `/uploads/{upload_id}/parts/{n}` | 上传分片；可选请求头 `X-Part-SHA256` 做分片级校验 |
| GET | `/uploads/{upload_id}/parts` | 查询已上传分片清单（断点续传据此跳过已完成分片） |
| POST | `/uploads/{upload_id}/complete` | 合并；body 可选 `{"parts": [1,2,...]}` 指定顺序，缺省按分片号升序 |
| DELETE | `/uploads/{upload_id}` | 中止会话并清理分片，返回 `204` |

### 回收

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/gc` | 同步执行一轮回收，返回统计（回收文件数/字节数/过期会话数） |

## 并发与版本化语义

- **版本不可变**：版本数据一旦落盘即不可变；覆盖写生成新版本，删除只是打标记，
  旧版本数据由 GC 异步回收。
- **读写快照隔离**：所有元数据变更在单锁保护下完成，数据文件先写临时文件再
  `os.replace` 原子落盘。并发读写同一 key 时，读者只会看到某个完整版本，
  绝不会读到写了一半的内容。
- **删除语义**（对齐 S3 版本化）：
  - 删除指定版本 → 该版本标记删除，普通读取回退到最近的可见版本；
  - 整 key 删除 → 写入删除标记，普通读取返回 404，历史版本仍可按版本号读取，
    不会"复活"旧版本。
- **分片幂等**：重复上传同一分片且内容一致 → 幂等成功；内容不一致 → `409 part_conflict`。
  并发上传同一分片时只有一个内容获胜，其余收到冲突错误。
- **重复合并**：会话成功合并后即销毁，再次合并返回 `404 no_such_upload`，
  不会生成第二个版本。
- **断点续传**：会话与分片持久化在磁盘上，进程重启后可用 `GET /uploads/{id}/parts`
  确认进度并继续上传。
- **合并校验**：合并前校验清单完整性（缺片拒绝），合并后校验声明的整体大小/SHA-256，
  任一不通过都拒绝且不产生任何版本——不会悄悄拼出损坏的对象。

## 保留规则与淘汰语义

- **淘汰只打标记**：维护轮次对命中规则的旧版本写入 `expired` 标记（普通读取随即不可见），
  数据文件随后被回收；版本条目仍保留在清单里，可审计。
- **绝不淘汰的版本**：
  - 当前对外可见的最新版本（即使所有版本都超龄，最新的也保留）；
  - 被显式冻结的版本（`PUT .../hold`）：冻结版本既不能被规则淘汰，也不能被删除
    （返回 `409 version_on_hold`），解冻后恢复正常；
  - 正在被读取的版本：读取与维护在同一把存储锁上互斥，读者拿到的一定是完整版本，
    维护绝不会删掉在读的数据。
- **三种"读不到"严格区分**：
  - 版本号根本不存在 → `404 no_such_version`；
  - 版本存在但被显式删除/回收 → `410 version_already_deleted`；
  - 版本存在但被保留规则淘汰/回收 → `410 version_expired`。
  - 淘汰不会"复活"旧版本：整 key 删除（删除标记）之后，普通读取依旧是 `404 no_such_key`。
- **淘汰是安全且幂等的**：元数据判定与标记在存储锁内完成；重跑维护不会重复淘汰、
  不会报错；规则删除或放宽后，已淘汰的版本不会自动恢复（符合"已按规则到期"的事实）。

## 自动维护的触发时机与并发安全

- **触发时机**：服务启动后后台守护线程按 `OBJECT_STORE_MAINTENANCE_INTERVAL`（默认 300s）
  周期运行；也可随时 `POST /maintenance/run` 手动触发。定时触发与手动触发互斥，
  上一轮没跑完时新的定时触发直接**跳过**（不排队、不重入），不会堆积维护任务。
- **不卡正常流量**：每轮维护中，元数据扫描/标记在存储锁内完成（很快），真正的数据文件
  删除在**锁外**执行，避免大文件 IO 阻塞读写与分片上传。
- **不会读到半成品 / 不会误删**：数据文件一律"先写临时文件再 `os.replace` 原子落盘、
  然后在锁内登记引用"；维护扫描只可能把"无任何存活引用"的文件当孤儿回收，
  因而写入中途崩溃留下的残留会被清，而正常写入绝不会被当孤儿。
- **该清的会清干净**：每轮同时处理过期版本、已删除数据、孤儿文件、过期上传会话；
  容量记账按"计划删除量"扣减，锁外删除若失败会对账补回，保证 `used_bytes` 不虚增虚减。
- **可观测**：最近运行时间、上一轮统计、累计轮次（持久化）、调度器是否在跑、
  上一轮错误都能通过 `GET /maintenance/status` 查看，后台日志也会打印每轮结果。

## 总容量上限判定规则

配置 `QuotaConfig.max_total_bytes`（或环境变量 `OBJECT_STORE_MAX_TOTAL_BYTES`）后，
对**整个存储**生效，占用 = 全部存活版本数据 + 分片数据 + 合并临时产物 + 上传占座。
四个写入环节分别校验，超限时返回 `507 total_capacity_exceeded`，`detail.scope` 标明环节：

| 环节 | scope | 判定 |
| --- | --- | --- |
| 整体写入 | `put_object` | `已用 + 本次大小 > 上限` 即拒，拒绝不落任何数据 |
| 创建分片会话 | `new_upload` | 声明了 `expected_size` 时按 **2 倍最终大小占座**（覆盖"分片 + 合并产物"同时在盘的瞬时峰值），放不下即拒 |
| 上传分片 | `upload_part` | 分片先抵扣占座，超出占座且使 `已用 + 新增 > 上限` 时拒绝该分片 |
| 合并分片 | `complete_upload` | 合并产物落定前按瞬时峰值再校验一次，放不下则拒绝且不产生版本，分片保留可重试 |

- 中止分片会话（`DELETE /uploads/{id}`）会释放占座与已传分片占用；
  合并成功后占座核销，最终只按合并出的版本大小计一份。
- **空间腾出来后自动恢复**：维护回收删除/过期数据后 `used_bytes` 立即下降，
  此前因容量不足被拒的写入无需任何额外操作即可重新成功。
- 重启后已用容量通过扫描磁盘重建（占座是内存记账，重启后未完成会话按其实际分片占用计）。
- 单对象大小、分片大小、分片数、同 key 并发会话数等**结构性配额**仍然独立生效
  （`413/429`），与总容量（`507`）互不混淆。

## 错误分类

| HTTP | code | 含义 |
| --- | --- | --- |
| 404 | `no_such_key` | key 不存在或已被整 key 删除 |
| 404 | `no_such_version` | 指定的版本号不存在 |
| 410 | `version_already_deleted` | 版本存在但已被显式删除（与 404 可区分） |
| 410 | `version_expired` | 版本存在但已被保留规则淘汰（与"不存在""已删除"都可区分） |
| 409 | `version_on_hold` | 版本被冻结，不能删除 |
| 400 | `retention_rule_invalid` | 保留规则参数非法（scope/pattern/max_* 取值不合法） |
| 404 | `no_such_upload` | 上传会话不存在（含已合并/已中止/被回收） |
| 409 | `upload_already_completed` | 会话已合并完成 |
| 409 | `part_conflict` | 同一分片被重复/并发上传且内容不一致 |
| 400 | `part_checksum_mismatch` | 分片内容校验失败 |
| 400 | `missing_parts` | 合并清单引用了未上传的分片（`detail.missing` 列出缺失分片号） |
| 400 | `complete_checksum_mismatch` | 合并结果与声明不符（`detail.reason` 区分 `size_mismatch`/`sha256_mismatch`） |
| 413 | `object_too_large` | 对象/分片/累计大小超结构性配额 |
| 413 | `too_many_parts` | 分片号超出上限 |
| 429 | `too_many_concurrent_uploads` | 同一 key 并发上传会话数超限 |
| 507 | `total_capacity_exceeded` | 整个存储总容量超限（`detail.scope` 区分 `put_object`/`new_upload`/`upload_part`/`complete_upload`） |

## 配额规则

配额由 `QuotaConfig` 配置（默认值见 `object_storage/models.py`）：

- `max_object_size`：单对象整体大小上限（PUT 体、声明大小、累计分片、合并结果四处都会校验）；
- `max_part_size`：单个分片大小上限；
- `max_parts_per_upload`：单次上传的分片号上限（1..N）；
- `max_concurrent_uploads`：同一 key 的活跃上传会话数上限（中止/合并/回收后释放名额）；
- `max_total_bytes`：整个存储的总容量上限（`None` 表示不限），判定规则见上一节。

所有超限都**明确拒绝**（对应上述错误码），不会静默丢弃或截断。

## 后台回收

`GarbageCollector.collect_once()` / `POST /gc` / `POST /maintenance/run` 执行一轮维护：

1. **保留规则淘汰**：命中 `max_versions` / `max_age_seconds` 的旧版本打 `expired` 标记
   （当前可见版本、冻结版本豁免）；
2. 回收已删除/已淘汰版本的数据文件（元数据条目保留，读错误语义不变）；
3. 回收无元数据引用的孤儿文件（如崩溃在"写文件"与"登记元数据"之间的残留）；
4. 中止并清理超过 `upload_ttl` 的未完成/残留上传会话。

元数据判定与标记全程在存储锁内、数据文件删除在锁外，**绝不触碰仍被版本清单引用、
被冻结或正在被读取的数据**；可重复执行（幂等），返回的统计说明每类处理了多少。

## 对外兼容范围

- 原有接口（对象读写、版本清单、分片上传、`POST /gc`）路径、请求/响应字段、错误码
  全部保持兼容；版本清单与写入响应在原字段之外**新增** `hold`、`expired`、
  `data_reclaimed` 字段（旧客户端忽略即可）。
- 新增接口均为新路径：`/retention`、`/capacity`、`/maintenance/*`、
  `/objects/{key}/versions/{vid}/hold`，不影响既有调用。
- 未配置 `max_total_bytes` 时总容量不限，行为与旧版一致；默认 `QuotaConfig` 未改变
  既有结构性配额默认值。
- 规则、版本冻结/淘汰状态、维护轮次都持久化在数据目录，服务重启后语义延续。

## 本地验证

```bash
uv run pytest                 # 全部 89 个用例，-v 输出每个用例结果
uv run pytest tests/test_retention.py      # 只看保留规则与淘汰
uv run pytest tests/test_capacity.py       # 只看总容量上限
uv run pytest tests/test_maintenance.py    # 只看自动维护与并发安全
uv run pytest -k multipart                 # 只看分片上传（确认旧行为无回归）
```

测试日志（`log_cli` 已开启）会打出每个用例覆盖的业务点（如"业务覆盖：并发读取下不误删"）
及开始/通过记录。覆盖范围：

- `tests/test_store.py`：分片主流程、断点续传（含跨进程重启恢复）、幂等重传、
  并发同一分片仲裁、并发读写快照隔离、分片/整体校验失败、版本化与删除语义、配额；
- `tests/test_gc.py`：删除版本回收、孤儿文件、过期会话、存活版本保护、回收幂等；
- `tests/test_retention.py`：规则增删改查与匹配优先级、参数校验、重启持久化、
  按版本数/时长淘汰、可见版本与冻结版本豁免、过期/删除/不存在三类错误区分、
  删除标记下不复活、多轮幂等；
- `tests/test_capacity.py`：四个写入环节的总容量拒绝（含 scope 区分）、占座与结算、
  中止释放、回收后写入恢复、重启容量重建；
- `tests/test_maintenance.py`：手动/定时触发、轮次互斥跳过、轮次持久化、
  单轮失败不杀调度器、回收与并发读互斥不误删、并发写入下记账一致；
- `tests/test_api.py` / `tests/test_maintenance_api.py`：全部 HTTP 契约、错误码分类、
  保留规则/冻结/容量/维护接口、端到端淘汰。

### 真启动一个服务手动走一遍

```bash
# 1) 以 2000 字节总容量、2 秒维护间隔启动
OBJECT_STORE_MAX_TOTAL_BYTES=2000 OBJECT_STORE_MAINTENANCE_INTERVAL=2 \
  uv run uvicorn main:app --port 8000 &

# 2) 配保留规则：logs/app.log 最多保留 2 个版本
curl -X PUT localhost:8000/retention -H 'content-type: application/json' \
  -d '{"scope":"key","pattern":"logs/app.log","max_versions":2}'

# 3) 连续写 3 个版本，立即触发一轮维护
for i in 1 2 3; do curl -X PUT --data-binary "version-$i" localhost:8000/objects/logs/app.log; done
curl -X POST localhost:8000/maintenance/run | jq .stats
#   => {"expired_versions":1,"reclaimed_version_files":1,...}

# 4) 被淘汰的版本 -> 410 version_expired；不存在的版本 -> 404 no_such_version；最新仍可读
OLD=$(curl -s localhost:8000/objects/logs/app.log/versions | jq -r '.versions[-1].version_id')
curl -i "localhost:8000/objects/logs/app.log?version_id=$OLD"   # HTTP/1.1 410, code=version_expired
curl localhost:8000/objects/logs/app.log                         # version-3

# 5) 冻结版本不可删（409 version_on_hold）
MID=$(curl -s localhost:8000/objects/logs/app.log/versions | jq -r '.versions[-2].version_id')
curl -X PUT "localhost:8000/objects/logs/app.log/versions/$MID/hold"
curl -i -X DELETE "localhost:8000/objects/logs/app.log?version_id=$MID"   # 409

# 6) 总容量：写满后被 507 拒绝（scope 可区分环节），删除+回收后又能写
head -c 1500 /dev/zero | tr '\0' x | curl -X PUT --data-binary @- localhost:8000/objects/big
head -c 900 /dev/zero  | tr '\0' y | curl -X PUT --data-binary @- localhost:8000/objects/mid  # 507
curl localhost:8000/capacity | jq
BIG=$(curl -s localhost:8000/objects/big/versions | jq -r '.versions[0].version_id')
curl -X DELETE "localhost:8000/objects/big?version_id=$BIG"; sleep 2.5
head -c 900 /dev/zero | tr '\0' y | curl -X PUT --data-binary @- localhost:8000/objects/mid  # 201

# 7) 看自动维护到底干没干活
curl localhost:8000/maintenance/status | jq '{total_runs,last_run_at,last_stats,scheduler_running,capacity}'
```

重启服务后：规则仍在（`retention.json`）、累计轮次接着计数（`maintenance_state.json`）、
被淘汰版本依旧返回 `version_expired`、容量按磁盘重新扫描。

## 目录结构

```
object_storage/
  errors.py       # 错误分类（稳定 code + HTTP 状态）
  models.py       # 数据模型、配额与保留规则配置
  store.py        # 核心存储：版本化读写、分片上传、容量记账、读保护、版本冻结
  retention.py    # 保留规则的持久化与匹配
  gc.py           # 保留淘汰 + 后台回收（锁内标记、锁外删文件）
  maintenance.py  # 自动维护调度器与可观测状态
  api.py          # FastAPI 路由
main.py           # 服务入口（接入总容量上限与自动维护）
tests/            # 单元/接口/并发测试
<data>/
  retention.json            # 保留规则（持久化）
  maintenance_state.json    # 维护累计轮次（持久化）
  objects/ uploads/         # 版本数据与分片
```
