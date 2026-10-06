# local-object-storage

用本地磁盘模拟对象存储后端的服务，支持**分片上传、多版本读写、配额限制与后台回收**，
在重复请求、并发冲突和异常中断下给出一致且可解释的结果。

## 快速开始

```bash
uv sync                                # 安装依赖
uv run uvicorn main:app --port 8000    # 启动服务（数据目录默认 ./data，可用 OBJECT_STORE_ROOT 覆盖）
uv run pytest                          # 运行全部测试（日志含各用例覆盖的业务点与执行结果）
```

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

## 错误分类

| HTTP | code | 含义 |
| --- | --- | --- |
| 404 | `no_such_key` | key 不存在或已被整 key 删除 |
| 404 | `no_such_version` | 指定的版本号不存在 |
| 410 | `version_already_deleted` | 版本存在但已被删除（与 404 可区分） |
| 404 | `no_such_upload` | 上传会话不存在（含已合并/已中止/被回收） |
| 409 | `upload_already_completed` | 会话已合并完成 |
| 409 | `part_conflict` | 同一分片被重复/并发上传且内容不一致 |
| 400 | `part_checksum_mismatch` | 分片内容校验失败 |
| 400 | `missing_parts` | 合并清单引用了未上传的分片（`detail.missing` 列出缺失分片号） |
| 400 | `complete_checksum_mismatch` | 合并结果与声明不符（`detail.reason` 区分 `size_mismatch`/`sha256_mismatch`） |
| 413 | `object_too_large` | 对象/分片/累计大小超配额 |
| 413 | `too_many_parts` | 分片号超出上限 |
| 429 | `too_many_concurrent_uploads` | 同一 key 并发上传会话数超限 |

## 配额规则

配额由 `QuotaConfig` 配置（默认值见 `object_storage/models.py`）：

- `max_object_size`：单对象整体大小上限（PUT 体、声明大小、累计分片、合并结果四处都会校验）；
- `max_part_size`：单个分片大小上限；
- `max_parts_per_upload`：单次上传的分片号上限（1..N）；
- `max_concurrent_uploads`：同一 key 的活跃上传会话数上限（中止/合并/回收后释放名额）。

所有超限都**明确拒绝**（对应上述错误码），不会静默丢弃或截断。

## 后台回收

`GarbageCollector.collect_once()`（或 `POST /gc`）执行一轮回收：

1. 回收元数据已标记删除的版本数据文件（元数据保留，已删除版本的错误语义不变）；
2. 回收无元数据引用的孤儿文件（如崩溃在"写文件"与"登记元数据"之间的残留）；
3. 中止并清理超过 `upload_ttl` 的未完成上传会话。

回收全程持有存储锁，与读写互斥，**绝不触碰仍被版本清单引用的数据**；
回收可重复执行（幂等），返回的统计说明每类回收了多少。

## 本地验证

```bash
uv run pytest                 # 全部 50 个用例，-v 输出每个用例结果
uv run pytest tests/test_store.py -k multipart   # 只看分片上传相关
```

测试日志（`log_cli` 已开启）会打出每个用例覆盖的业务点（如"业务覆盖：并发上传同一分片"）
及开始/通过记录。覆盖范围：

- `tests/test_store.py`：分片主流程、断点续传（含跨进程重启恢复）、幂等重传、
  并发同一分片仲裁、并发读写快照隔离、分片/整体校验失败、版本化与删除语义、配额；
- `tests/test_api.py`：HTTP 契约、错误码分类、分片全流程、校验头、配额状态码；
- `tests/test_gc.py`：删除版本回收、孤儿文件、过期会话、存活版本保护、回收幂等。

手工验证示例：

```bash
uv run uvicorn main:app --port 8000 &
curl -X PUT --data-binary 'hello' localhost:8000/objects/demo.txt
curl localhost:8000/objects/demo.txt
UID=$(curl -s -X POST 'localhost:8000/uploads?key=big.bin&expected_size=6' | jq -r .upload_id)
curl -X PUT --data-binary 'foo' localhost:8000/uploads/$UID/parts/1
curl -X PUT --data-binary 'bar' localhost:8000/uploads/$UID/parts/2
curl -s localhost:8000/uploads/$UID/parts | jq
curl -X POST -H 'content-type: application/json' -d '{}' localhost:8000/uploads/$UID/complete
curl localhost:8000/objects/big.bin   # => foobar
```

## 目录结构

```
object_storage/
  errors.py   # 错误分类（稳定 code + HTTP 状态）
  models.py   # 数据模型与配额配置
  store.py    # 核心存储：版本化读写、分片上传、配额
  gc.py       # 后台回收
  api.py      # FastAPI 路由
main.py       # 服务入口
tests/        # 单元测试
```
