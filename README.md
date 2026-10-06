# 本地对象存储（local-object-storage）

用本地磁盘模拟对象存储后端，在单进程内提供**分片上传 / 断点续传、多版本
并发读写、配额控制与垃圾回收**，并对重复请求、并发冲突、异常中断给出一致
且可解释的结果。

- 语言：Python 3.14（仅依赖 FastAPI/uvicorn，核心存储零第三方依赖）
- 数据全部落在本地目录，结构即日志，便于排查
- 所有失败都带稳定错误码（`code`）与 HTTP 状态码，原因可区分

---

## 1. 快速开始

```bash
uv sync                       # 安装依赖
uv run pytest                 # 运行全部单元测试（53 个，分 7 个业务组）
OBJECT_STORAGE_DIR=./.data uv run python main.py   # 启动 HTTP 服务（默认 127.0.0.1:8000）
```

启动后：

- 交互文档：<http://127.0.0.1:8000/docs>
- 健康检查：`GET /health`

### 1.1 直接用 Python API

```python
import io
from object_storage.service import ObjectStorageService
from object_storage.config import StorageConfig

svc = ObjectStorageService("./.data").start()

v = svc.put_object("docs/a.txt", io.BytesIO(b"hello"))
record, stream = svc.get_object("docs/a.txt")
print(stream.read())                      # b"hello"
svc.shutdown()
```

---

## 2. HTTP 接口契约

所有错误响应体统一为：

```json
{ "code": "BadDigest", "message": "...", "details": { ... } }
```

### 2.1 简单对象

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/objects/{key}` | 整体上传（body 即对象字节），成功返回 201 与新版本信息。可带请求头 `X-Checksum-Sha256` 声明 sha256（hex），服务端复算不一致返回 `BadDigest`。 |
| `GET` | `/objects/{key}?versionId=...` | 读取对象；不带 `versionId` 读当前版本。响应头 `X-Version-Id` / `X-Checksum-Sha256` / `X-Object-Size`。 |
| `HEAD` | 同上 | 只取元信息响应头。 |
| `DELETE` | `/objects/{key}?versionId=...` | 不带 `versionId`：给 key 追加墓碑版本，当前不可见。带 `versionId`：删除指定版本（墓碑）。 |
| `GET` | `/objects/{key}/versions` | 列出该 key 的全部版本（含墓碑）。 |

### 2.2 分片上传

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/multipart/uploads` | body：`{"key": "...", "partSize": 5242880, "metadata": {}}`，返回 `uploadId`。 |
| `PUT` | `/multipart/uploads/{uploadId}/parts/{partNumber}` | 上传一个分片（1 ≤ partNumber）。可带 `X-Checksum-Sha256`。 |
| `GET` | `/multipart/uploads/{uploadId}/parts` | 查询分片清单（断点续传依据）。 |
| `GET` | `/multipart/uploads/{uploadId}` | 查询会话状态。 |
| `POST` | `/multipart/uploads/{uploadId}/complete` | body：`{"checksumSha256": "..."}`（可选）；校验通过后合并并提交新版本。 |
| `DELETE` | `/multipart/uploads/{uploadId}` | 中止上传（释放并发槽，分片 blob 由 GC 回收）。 |
| `GET` | `/multipart/uploads` | 列出进行中的上传会话。 |

### 2.3 管理接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/admin/gc` | 立即执行一次回收，返回统计。 |

---

## 3. 核心语义

### 3.1 分片上传与断点续传

1. `begin` 生成 `uploadId`，会话元数据持久化在 `uploads/<uploadId>.json`，
   写入走**临时文件 + 原子 rename**；
2. 每个分片以 **sha256 内容寻址**落盘（`blobs/ab/abcd...`），天然去重；
3. 进程崩溃 / 客户端中断后，会话与已传分片都还在：重新
   `GET .../parts` 得到清单，只补传缺失分片即可（断点续传）；
4. `complete` 时依次做以下校验，任一不过都**拒绝合并、不产生版本**：
   - 分片必须从 1 开始且编号连续，缺口通过 `details.missing` 返回
     （`MissingPart`，400）；
   - 除最后一个分片外，每片必须 ≥ 初始化声明的 `partSize`
     （`EntityTooSmall`，400）；
   - 合并后整体 sha256 若与 `checksumSha256` 不符，返回
     `InvalidObjectChecksum`（400），合并 blob 不被引用。

### 3.2 幂等与并发

- **重复上传同一分片，字节相同**：返回既有分片记录（同 `etag`），不重复
  计数，不落第二份数据；
- **并发上传同一分片，字节相同**：会话锁串行化，首写生效，其余请求拿到
  同一结果；
- **同一编号上传不同内容**：首写为准，后续请求得到 `BadDigest`（400），
  绝不会静默覆盖；
- **重复 / 并发 `complete`**：同一会话只拼接一次、只产生一个版本，重复
  请求返回同一 `versionId`；
- 分片声明了错误的 `X-Checksum-Sha256` 时，数据先写临时文件，复算不过
  即删除，**脏数据绝不进入 blob 目录**。

### 3.3 多版本读写

- 每次 `PUT` 或成功的分片合并都会追加一个不可变版本，并原子切换
  "current" 指针；
- 版本内容（blob）不可变，元数据文件原子替换，配合 key 级互斥锁：
  并发读写同一个 key 时，读者要么看到旧版本、要么看到完整的新版本，
  **绝不会读到写了一半的内容**；
- 可按 `versionId` 读取历史版本，或删除指定版本；
- 版本 id 形如 `20261006T173621-838d844863cb`，时间前缀保证可排序。

### 3.4 删除与错误分类

| 场景 | code | HTTP | 上层判断 |
| --- | --- | --- | --- |
| key 从未存在 / 当前无存活版本 | `NoSuchKey` | 404 | 对象不存在 |
| `versionId` 从未存在 | `NoSuchVersion` | 404 | 版本号写错 |
| `versionId` 存在但已删除（墓碑） | `VersionDeleted` | 410 | 曾存在、已删 |
| 上传会话不存在 | `NoSuchUpload` | 404 | |
| 合并一个已完成会话 | 幂等成功 | 200 | 重复请求 |
| 给已完成会话传片 / 中止 | `UploadAlreadyCompleted` | 409 | |
| 分片缺失 | `MissingPart` | 400 | `details.missing` |
| 非末片过小 | `EntityTooSmall` | 400 | `details.partNumber` |
| 分片 checksum 不符 | `BadDigest` | 400 | expected/actual |
| 整对象 checksum 不符 | `InvalidObjectChecksum` | 400 | expected/actual |
| 单对象超总大小 | `EntityTooLarge` | 413 | |
| 分片数超限 | `TooManyParts` | 400/413 | |
| 分片号越界 | `InvalidPartNumber` | 400 | |
| 请求体非法 / 空 key | `InvalidRequest` | 400 | |
| 并发上传会话数超限 | `SlowDown` | 429 | 稍后重试 |
| 元数据引用的 blob 缺失/损坏 | `StorageCorruption` | 500 | 磁盘异常 |

### 3.5 配额

由 `StorageConfig` 配置，超限一律**显式拒绝**，绝不静默截断：

| 配额 | 默认值 | 触发错误 |
| --- | --- | --- |
| `max_object_size`（单对象总大小） | 5 GiB | `EntityTooLarge`（413） |
| `max_parts_per_upload`（分片数） | 10,000 | `TooManyParts` |
| `max_concurrent_uploads`（进行中会话数） | 100 | `SlowDown`（429） |
| `min_part_size`（非末片最小字节） | 5 MiB | `EntityTooSmall` |

总大小在分片上传过程中按累计值即时检查；并发槽在 `begin` 时占用，
`complete`/`abort` 时释放，进程重启后会按磁盘上的会话文件重建计数。

### 3.6 垃圾回收

**回收时机（可解释）**：

1. 显式调用 `POST /admin/gc` 或 `service.garbage_collect()`；
2. 后台守护线程按 `gc_interval_seconds`（默认 60s）周期执行，可通过
   `enable_background_gc=False` 关闭；
3. 每次 sweep 顺带中止超过 `multipart_ttl_seconds`（默认 24h）的未完成
   上传，并清理已完成且过了幂等保护窗口的会话文件与崩溃残留临时文件。

**安全性（不影响仍被引用的版本）**：

- sweep 先扫描全部对象版本元数据 + 进行中分片，构造 blob 引用集合，
  再删除磁盘上的孤儿 blob；
- 被覆盖但未删除的旧版本仍被版本元数据引用 → 保留；
- 内容相同的多个 key / 分片共享同一 blob，只要还有任一引用就不删；
- sweep 自身串行（加锁），删除前会再确认一次引用快照。

返回统计示例：

```json
{
  "abortedUploads": 0,
  "purgedCompletedSessions": 1,
  "referencedBlobs": 4,
  "removedBlobs": 2,
  "keptBlobs": 4,
  "removedTmpFiles": 0,
  "bytesFreed": 2048,
  "elapsedSeconds": 0.0012
}
```

---

## 4. 磁盘布局

```
<data_dir>/
├── blobs/                      # 内容寻址的不可变数据块
│   └── ab/
│       └── ab12...ef           # 文件名 = sha256(字节)
├── tmp/                        # 写入/合并的临时文件（崩溃可清）
├── uploads/
│   ├── <uploadId>.json         # 分片会话清单（原子替换）
│   └── <uploadId>.completed.json  # 合并结果（幂等重试依据）
└── objects/
    └── <safe(key)>.json        # key -> 版本列表 + current 指针
```

---

## 5. 本地验证

### 5.1 单元测试

```bash
uv run pytest            # 详细模式：每个用例一行结果 + 业务分组汇总
uv run pytest -q         # 简洁模式
```

测试按业务分 7 组（pytest marker），运行日志开头打印覆盖了哪些业务，
结尾给出每组的通过 / 失败 / 错误统计：

| Marker / 业务组 | 覆盖内容 |
| --- | --- |
| `multipart` | 初始化、分片上传、清单查询、合并、读取主流程 |
| `resume` | 重启后续传、重复传片幂等、并发同片幂等、坏 checksum 重试、重复合并幂等 |
| `validation` | 缺片、非末片过小、分片/整对象 checksum 不符、大小/片数/并发配额 |
| `versions` | 覆盖出版本、按版本读、删除指定版本/整 key、404/410 区分、重启持久化 |
| `concurrency` | 并发覆盖写只读到完整版本、并发分片、并发合并只产生一个版本 |
| `gc` | 旧版本/中止分片回收、共享引用保护、过期会话中止、并发 sweep、后台 GC |
| `api` | HTTP 契约、响应头、全部关键错误码 |

### 5.2 curl 手测

```bash
OBJECT_STORAGE_DIR=./.data uv run python main.py &

# 分片上传
UID_=$(curl -s -X POST localhost:8000/multipart/uploads \
  -H 'Content-Type: application/json' \
  -d '{"key":"big.bin","partSize":5}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["uploadId"])')
printf 'AAAAA' | curl -s -X PUT localhost:8000/multipart/uploads/$UID_/parts/1 --data-binary @-
printf 'BBB'   | curl -s -X PUT localhost:8000/multipart/uploads/$UID_/parts/2 --data-binary @-
CK=$(printf 'AAAAABBB' | sha256sum | cut -d' ' -f1)
curl -s -X POST localhost:8000/multipart/uploads/$UID_/complete \
  -H 'Content-Type: application/json' -d "{\"checksumSha256\":\"$CK\"}"
curl -s localhost:8000/objects/big.bin          # AAAABBB
curl -s -X POST localhost:8000/admin/gc         # 手动回收
```

---

## 6. 设计说明与边界

- 锁粒度为进程内的 **key / uploadId 级 RLock**，因此一致性保证的作用域
  是"单服务实例 + 单数据目录"；多实例共享同一目录不在当前支持范围内。
- 合并采用"先拼接新 blob → 校验 → 原子切换元数据"顺序，任何一步中断：
  - 已写的 blob 成为孤儿，由下次 GC 回收；
  - 未切换的版本对读者完全不可见，重试 `complete` 即可。
- 删除指定版本后版本记录保留为墓碑（返回 410），其 blob 引用立即释放；
  若需彻底擦除墓碑记录，可由运维在确认合规后清理 `objects/*.json`。
