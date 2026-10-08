"""核心存储：本地磁盘模拟对象存储，支持版本化与分片上传。

一致性设计要点：
- 所有元数据变更都在 ``self._lock`` 保护下完成，数据文件先写临时文件再
  ``os.replace`` 原子落盘，因此读者只会看到某个完整版本，绝不会读到写了一半的内容。
- 版本一旦写入即不可变；覆盖写生成新版本，删除只是打标记，数据文件由 GC 异步回收。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote, unquote

from .errors import (
    CompleteChecksumMismatch,
    MissingParts,
    NoSuchKey,
    NoSuchUpload,
    NoSuchVersion,
    ObjectTooLarge,
    PartChecksumMismatch,
    PartConflict,
    TooManyConcurrentUploads,
    TooManyParts,
    TotalCapacityExceeded,
    UploadAlreadyCompleted,
    VersionAlreadyDeleted,
    VersionExpired,
    VersionOnHold,
)
from .models import PartInfo, QuotaConfig, UploadSession, VersionInfo


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class LocalObjectStore:
    """以本地目录为后端的版本化对象存储。

    目录布局::

        root/
          objects/<urlquote(key)>/versions/<version_id>   # 版本数据（不可变）
          objects/<urlquote(key)>/meta.json               # 版本清单
          uploads/<upload_id>/parts/<part_number>         # 分片数据
          uploads/<upload_id>/meta.json                   # 上传会话元数据
    """

    def __init__(self, root: str | Path, quota: QuotaConfig | None = None):
        self.root = Path(root)
        self.quota = quota or QuotaConfig()
        self._lock = threading.RLock()
        # 正在被读取的版本坐标集合 {(key, version_id)}；回收/淘汰必须跳过这些版本，
        # 即使元数据已标记删除也不能把数据文件删掉（读保护）。
        self._active_reads: set[tuple[str, str]] = set()
        (self.root / "objects").mkdir(parents=True, exist_ok=True)
        (self.root / "uploads").mkdir(parents=True, exist_ok=True)
        # 已用容量（字节）：重启后扫描磁盘重建，正常运行中随写入/回收增量维护。
        self._used_bytes = self._scan_used_bytes()

    def _scan_used_bytes(self) -> int:
        """统计当前数据占用（版本文件 + 分片文件），重启重建容量基线。"""
        total = 0
        for base in (self.root / "objects", self.root / "uploads"):
            if not base.exists():
                continue
            for dirpath, _dirs, files in os.walk(base):
                for name in files:
                    if name == "meta.json" or name.startswith(".tmp-") or name.startswith("."):
                        continue
                    try:
                        total += (Path(dirpath) / name).stat().st_size
                    except FileNotFoundError:
                        pass
        return total

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used_bytes

    def capacity(self) -> dict:
        """总容量判定信息（供可观测接口使用）。"""
        with self._lock:
            return {
                "used_bytes": self._used_bytes,
                "limit_bytes": self.quota.max_total_bytes,
                "available_bytes": (
                    None if self.quota.max_total_bytes is None
                    else max(0, self.quota.max_total_bytes - self._used_bytes)
                ),
                "over_limit": (
                    False if self.quota.max_total_bytes is None
                    else self._used_bytes > self.quota.max_total_bytes
                ),
            }

    def _check_total_capacity(self, incoming: int, scope: str) -> None:
        """总容量上限校验：占用 + 新增 > 上限即明确拒绝（不在任何环节吞数据）。"""
        limit = self.quota.max_total_bytes
        if limit is None:
            return
        if self._used_bytes + incoming > limit:
            raise TotalCapacityExceeded(
                f"总容量不足：已用 {self._used_bytes} + 申请 {incoming} > 上限 {limit}",
                detail={
                    "scope": scope,
                    "used": self._used_bytes,
                    "limit": limit,
                    "requested": incoming,
                },
            )

    # ------------------------------------------------------------------
    # 路径与元数据辅助
    # ------------------------------------------------------------------
    def _object_dir(self, key: str) -> Path:
        return self.root / "objects" / quote(key, safe="")

    def _upload_dir(self, upload_id: str) -> Path:
        return self.root / "uploads" / upload_id

    @staticmethod
    def _atomic_write(path: Path, data: bytes) -> None:
        """先写临时文件再 rename，保证落盘原子性。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def _load_meta(self, key: str) -> dict:
        meta_path = self._object_dir(key) / "meta.json"
        if not meta_path.exists():
            return {"key": key, "versions": []}
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def _save_meta(self, key: str, meta: dict) -> None:
        self._atomic_write(
            self._object_dir(key) / "meta.json",
            json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"),
        )

    def _load_upload_meta(self, upload_id: str) -> dict:
        meta_path = self._upload_dir(upload_id) / "meta.json"
        if not meta_path.exists():
            raise NoSuchUpload(f"upload {upload_id} 不存在", detail={"upload_id": upload_id})
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def _save_upload_meta(self, upload_id: str, meta: dict) -> None:
        self._atomic_write(
            self._upload_dir(upload_id) / "meta.json",
            json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"),
        )

    @staticmethod
    def _version_from_dict(key: str, v: dict) -> VersionInfo:
        return VersionInfo(
            version_id=v["version_id"],
            key=key,
            size=v["size"],
            sha256=v["sha256"],
            created_at=v["created_at"],
            deleted=v.get("deleted", False),
            delete_marker=v.get("delete_marker", False),
            data_reclaimed=v.get("data_reclaimed", False),
            expired=v.get("expired", False),
            hold=v.get("hold", False),
        )

    # ------------------------------------------------------------------
    # 简单读写（整体对象）
    # ------------------------------------------------------------------
    def put_object(self, key: str, data: bytes) -> VersionInfo:
        """覆盖写：生成新版本。旧版本保留，直到被删除或被 GC 回收。"""
        if len(data) > self.quota.max_object_size:
            raise ObjectTooLarge(
                f"对象大小 {len(data)} 超过上限 {self.quota.max_object_size}",
                detail={"size": len(data), "limit": self.quota.max_object_size},
            )
        digest = _sha256(data)
        with self._lock:
            self._check_total_capacity(len(data), "put_object")
            version_id = uuid.uuid4().hex
            # _atomic_write 失败会自删临时文件；记账在落盘成功之后，失败即不占用容量。
            self._atomic_write(self._object_dir(key) / "versions" / version_id, data)
            self._used_bytes += len(data)
            meta = self._load_meta(key)
            entry = {
                "version_id": version_id,
                "size": len(data),
                "sha256": digest,
                "created_at": time.time(),
                "deleted": False,
                "delete_marker": False,
                "hold": False,
                "expired": False,
                "data_reclaimed": False,
            }
            meta["versions"].append(entry)
            self._save_meta(key, meta)
            return self._version_from_dict(key, entry)

    def get_object(self, key: str, version_id: str | None = None) -> tuple[VersionInfo, bytes]:
        """读取对象。不指定版本时读最新的可见版本。

        读取期间该版本登记为活跃读，任何回收/淘汰都不会删掉它的数据文件。
        显式删除 -> ``version_already_deleted``；保留规则淘汰 -> ``version_expired``；
        版本号根本不存在 -> ``no_such_version``，三者互不混淆。
        """
        with self._lock:
            meta = self._load_meta(key)
            versions = meta["versions"]
            if not versions:
                raise NoSuchKey(f"key {key!r} 不存在", detail={"key": key})
            if version_id is not None:
                entry = next((v for v in versions if v["version_id"] == version_id), None)
                if entry is None:
                    raise NoSuchVersion(
                        f"key {key!r} 没有版本 {version_id}",
                        detail={"key": key, "version_id": version_id},
                    )
            else:
                # 从最新往回找：遇到删除标记则 key 逻辑删除（不会复活旧版本）；
                # 跳过被单独删除/淘汰的版本，落在最近的可见版本上。
                entry = None
                for v in reversed(versions):
                    if v.get("delete_marker"):
                        break
                    if not v.get("deleted"):
                        entry = v
                        break
                if entry is None:
                    raise NoSuchKey(f"key {key!r} 已删除", detail={"key": key})
            # 走到这里 entry 是确定要读的版本（可能已被删除/淘汰）。
            if entry.get("delete_marker") or entry.get("deleted"):
                if entry.get("expired"):
                    raise VersionExpired(
                        f"版本 {entry['version_id']} 已被保留规则淘汰",
                        detail={"key": key, "version_id": entry["version_id"]},
                    )
                raise VersionAlreadyDeleted(
                    f"版本 {entry['version_id']} 已被删除",
                    detail={"key": key, "version_id": entry["version_id"]},
                )
            data_path = self._object_dir(key) / "versions" / entry["version_id"]
            if not data_path.exists():
                # 数据已被 GC 回收但元数据标记为可见：属于内部不一致，按已删除处理。
                raise VersionAlreadyDeleted(
                    f"版本 {entry['version_id']} 的数据已被回收",
                    detail={"key": key, "version_id": entry["version_id"]},
                )
            # 登记活跃读并在同一把锁内读完文件：回收收集阶段同样持锁，
            # 因此活跃读期间文件绝不可能进入待删列表（读完才会放锁）。
            coord = (key, entry["version_id"])
            self._active_reads.add(coord)
            try:
                return self._version_from_dict(key, entry), data_path.read_bytes()
            finally:
                self._active_reads.discard(coord)

    def delete_object(self, key: str, version_id: str | None = None) -> VersionInfo:
        """删除对象。

        - 指定版本：将该版本标记为已删除（数据文件留给 GC 回收）。
        - 不指定版本：写入删除标记（delete marker），key 逻辑上不可见，
          历史版本仍可按版本号读取。
        """
        with self._lock:
            meta = self._load_meta(key)
            versions = meta["versions"]
            if version_id is not None:
                entry = next((v for v in versions if v["version_id"] == version_id), None)
                if entry is None:
                    raise NoSuchVersion(
                        f"key {key!r} 没有版本 {version_id}",
                        detail={"key": key, "version_id": version_id},
                    )
                if entry.get("deleted") or entry.get("delete_marker"):
                    if entry.get("expired"):
                        raise VersionExpired(
                            f"版本 {version_id} 已被保留规则淘汰",
                            detail={"key": key, "version_id": version_id},
                        )
                    raise VersionAlreadyDeleted(
                        f"版本 {version_id} 已被删除",
                        detail={"key": key, "version_id": version_id},
                    )
                if entry.get("hold"):
                    raise VersionOnHold(
                        f"版本 {version_id} 已被冻结，不能删除",
                        detail={"key": key, "version_id": version_id},
                    )
                entry["deleted"] = True
                self._save_meta(key, meta)
                return self._version_from_dict(key, entry)
            if not versions:
                raise NoSuchKey(f"key {key!r} 不存在", detail={"key": key})
            marker = {
                "version_id": uuid.uuid4().hex,
                "size": 0,
                "sha256": "",
                "created_at": time.time(),
                "deleted": True,
                "delete_marker": True,
            }
            versions.append(marker)
            self._save_meta(key, meta)
            return self._version_from_dict(key, marker)

    def list_versions(self, key: str) -> list[VersionInfo]:
        """列出版本，最新在前（含删除标记与已删除版本）。"""
        with self._lock:
            meta = self._load_meta(key)
            if not meta["versions"] and not self._object_dir(key).exists():
                raise NoSuchKey(f"key {key!r} 不存在", detail={"key": key})
            return [self._version_from_dict(key, v) for v in reversed(meta["versions"])]

    def set_version_hold(self, key: str, version_id: str, hold: bool) -> VersionInfo:
        """显式冻结/解冻某个版本。被冻结的版本不会被删除，也不会被保留规则淘汰。"""
        with self._lock:
            meta = self._load_meta(key)
            entry = next((v for v in meta["versions"] if v["version_id"] == version_id), None)
            if entry is None:
                raise NoSuchVersion(
                    f"key {key!r} 没有版本 {version_id}",
                    detail={"key": key, "version_id": version_id},
                )
            if entry.get("deleted") or entry.get("delete_marker"):
                if entry.get("expired"):
                    raise VersionExpired(
                        f"版本 {version_id} 已被保留规则淘汰",
                        detail={"key": key, "version_id": version_id},
                    )
                raise VersionAlreadyDeleted(
                    f"版本 {version_id} 已被删除",
                    detail={"key": key, "version_id": version_id},
                )
            if entry.get("hold") != hold:
                entry["hold"] = hold
                self._save_meta(key, meta)
            return self._version_from_dict(key, entry)

    # ------------------------------------------------------------------
    # 分片上传
    # ------------------------------------------------------------------
    def create_multipart_upload(
        self, key: str, *, expected_size: int | None = None, expected_sha256: str | None = None
    ) -> UploadSession:
        """创建分片上传会话，可声明整体大小/校验值以便合并时校验。"""
        if expected_size is not None and expected_size > self.quota.max_object_size:
            raise ObjectTooLarge(
                f"声明的对象大小 {expected_size} 超过上限 {self.quota.max_object_size}",
                detail={"size": expected_size, "limit": self.quota.max_object_size},
            )
        with self._lock:
            # 新会话：若声明了整体大小，按「2 倍最终大小」占座——合并的瞬间
            # 分片数据与合并产物同时在盘上，预留覆盖这个瞬时峰值，使已声明的
            # 上传不会传到最后一步才被容量拒绝。
            reservation = (expected_size * 2) if expected_size else 0
            self._check_total_capacity(reservation, "new_upload")
            active = 0
            uploads_dir = self.root / "uploads"
            for child in uploads_dir.iterdir():
                meta_path = child / "meta.json"
                if not meta_path.exists():
                    continue
                m = json.loads(meta_path.read_text(encoding="utf-8"))
                if m["key"] == key and not m.get("completed"):
                    active += 1
            if active >= self.quota.max_concurrent_uploads:
                raise TooManyConcurrentUploads(
                    f"key {key!r} 的并发上传数达到上限 {self.quota.max_concurrent_uploads}",
                    detail={"key": key, "limit": self.quota.max_concurrent_uploads},
                )
            upload_id = uuid.uuid4().hex
            reserved = reservation  # 覆盖「分片 + 合并产物」瞬时峰值的占座
            self._used_bytes += reserved
            session_meta = {
                "upload_id": upload_id,
                "key": key,
                "created_at": time.time(),
                "expected_size": expected_size,
                "expected_sha256": expected_sha256,
                "parts": {},
                "completed": False,
                "reserved_bytes": reserved,
            }
            try:
                self._save_upload_meta(upload_id, session_meta)
                (self._upload_dir(upload_id) / "parts").mkdir(parents=True, exist_ok=True)
            except BaseException:
                # 落盘失败必须回滚占座，避免容量被一次没建成的会话永久占用。
                self._used_bytes -= reserved
                raise
            return UploadSession(
                upload_id=upload_id,
                key=key,
                created_at=session_meta["created_at"],
                expected_size=expected_size,
                expected_sha256=expected_sha256,
            )

    def upload_part(
        self, upload_id: str, part_number: int, data: bytes, *, sha256: str | None = None
    ) -> PartInfo:
        """上传分片。同一分片重复上传相同内容幂等成功；内容不同则报 PartConflict。"""
        digest = _sha256(data)
        if sha256 is not None and sha256 != digest:
            raise PartChecksumMismatch(
                f"分片 {part_number} 校验失败：声明 {sha256}，实际 {digest}",
                detail={"part_number": part_number, "expected": sha256, "actual": digest},
            )
        if not 1 <= part_number <= self.quota.max_parts_per_upload:
            raise TooManyParts(
                f"分片号 {part_number} 超出允许范围 1..{self.quota.max_parts_per_upload}",
                detail={"part_number": part_number, "limit": self.quota.max_parts_per_upload},
            )
        if len(data) > self.quota.max_part_size:
            raise ObjectTooLarge(
                f"分片大小 {len(data)} 超过单分片上限 {self.quota.max_part_size}",
                detail={"size": len(data), "limit": self.quota.max_part_size},
            )
        with self._lock:
            meta = self._load_upload_meta(upload_id)
            if meta.get("completed"):
                raise UploadAlreadyCompleted(
                    f"upload {upload_id} 已合并完成", detail={"upload_id": upload_id}
                )
            parts = meta["parts"]
            key = str(part_number)
            if key in parts:
                existing = parts[key]
                if existing["sha256"] == digest:
                    # 幂等重试：内容一致，直接返回已记录的分片信息。
                    return PartInfo(
                        part_number=part_number,
                        size=existing["size"],
                        sha256=existing["sha256"],
                        uploaded_at=existing["uploaded_at"],
                    )
                raise PartConflict(
                    f"分片 {part_number} 已存在且内容不一致",
                    detail={"part_number": part_number, "existing_sha256": existing["sha256"], "new_sha256": digest},
                )
            total = sum(p["size"] for p in parts.values()) + len(data)
            if total > self.quota.max_object_size:
                raise ObjectTooLarge(
                    f"累计分片大小 {total} 超过对象上限 {self.quota.max_object_size}",
                    detail={"size": total, "limit": self.quota.max_object_size},
                )
            # 总容量：占座中尚未被既有分片用掉的部分（unused_reserve）
            # 可覆盖当前分片，只有超出占座的字节才需要新增空间。
            charged_so_far = sum(p["size"] for p in parts.values())
            unused_reserve = max(0, int(meta.get("reserved_bytes", 0)) - charged_so_far)
            extra = max(0, len(data) - unused_reserve)
            self._check_total_capacity(extra, "upload_part")
            self._atomic_write(self._upload_dir(upload_id) / "parts" / str(part_number), data)
            self._used_bytes += extra
            parts[key] = {"size": len(data), "sha256": digest, "uploaded_at": time.time()}
            self._save_upload_meta(upload_id, meta)
            p = parts[key]
            return PartInfo(part_number=part_number, size=p["size"], sha256=p["sha256"], uploaded_at=p["uploaded_at"])

    def list_parts(self, upload_id: str) -> list[PartInfo]:
        """查询已上传的分片清单（断点续传时用于确认进度）。"""
        with self._lock:
            meta = self._load_upload_meta(upload_id)
            return [
                PartInfo(part_number=int(num), size=p["size"], sha256=p["sha256"], uploaded_at=p["uploaded_at"])
                for num, p in sorted(meta["parts"].items(), key=lambda kv: int(kv[0]))
            ]

    def complete_multipart_upload(self, upload_id: str, parts: list[int] | None = None) -> VersionInfo:
        """合并分片生成新版本。

        - 清单引用了未上传的分片 → MissingParts；
        - 合并结果与声明的大小/校验不一致 → CompleteChecksumMismatch；
        - 重复合并同一会话 → UploadAlreadyCompleted（不会生成第二个版本）。
        """
        with self._lock:
            meta = self._load_upload_meta(upload_id)
            if meta.get("completed"):
                raise UploadAlreadyCompleted(
                    f"upload {upload_id} 已合并完成", detail={"upload_id": upload_id}
                )
            available = meta["parts"]
            wanted = [str(n) for n in parts] if parts is not None else sorted(available, key=int)
            missing = [n for n in wanted if n not in available]
            if missing:
                raise MissingParts(
                    f"合并清单缺少分片: {missing}",
                    detail={"upload_id": upload_id, "missing": [int(n) for n in missing]},
                )
            if not wanted:
                raise MissingParts("合并清单为空", detail={"upload_id": upload_id})

            key = meta["key"]
            version_id = uuid.uuid4().hex
            version_path = self._object_dir(key) / "versions" / version_id
            version_path.parent.mkdir(parents=True, exist_ok=True)

            # 在临时文件中合并，全部校验通过后才原子替换为正式版本。
            hasher = hashlib.sha256()
            total = 0
            fd, tmp = tempfile.mkstemp(dir=version_path.parent, prefix=".tmp-")
            try:
                with os.fdopen(fd, "wb") as out:
                    for num in wanted:
                        chunk = (self._upload_dir(upload_id) / "parts" / num).read_bytes()
                        if _sha256(chunk) != available[num]["sha256"]:
                            raise CompleteChecksumMismatch(
                                f"分片 {num} 磁盘内容与会话记录不一致",
                                detail={"upload_id": upload_id, "part_number": int(num)},
                            )
                        out.write(chunk)
                        hasher.update(chunk)
                        total += len(chunk)
                    out.flush()
                    os.fsync(out.fileno())
                digest = hasher.hexdigest()
                if total > self.quota.max_object_size:
                    raise ObjectTooLarge(
                        f"合并后大小 {total} 超过上限 {self.quota.max_object_size}",
                        detail={"size": total, "limit": self.quota.max_object_size},
                    )
                # 合并产物与全部分片此刻同时在盘上。占座中尚未被分片用掉的
                # 部分（unused_reserve，声明场景等于最终大小）可抵扣新增产物。
                reserved = int(meta.get("reserved_bytes", 0))
                charged_parts = sum(p["size"] for p in available.values())
                unused_reserve = max(0, reserved - charged_parts)
                self._check_total_capacity(max(0, total - unused_reserve), "complete_upload")
                if meta.get("expected_size") is not None and total != meta["expected_size"]:
                    raise CompleteChecksumMismatch(
                        f"合并后大小 {total} 与声明的 {meta['expected_size']} 不一致",
                        detail={"reason": "size_mismatch", "expected": meta["expected_size"], "actual": total},
                    )
                if meta.get("expected_sha256") is not None and digest != meta["expected_sha256"]:
                    raise CompleteChecksumMismatch(
                        "合并后整体校验不一致",
                        detail={"reason": "sha256_mismatch", "expected": meta["expected_sha256"], "actual": digest},
                    )
                os.replace(tmp, version_path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except FileNotFoundError:
                    pass
                raise

            entry = {
                "version_id": version_id,
                "size": total,
                "sha256": digest,
                "created_at": time.time(),
                "deleted": False,
                "delete_marker": False,
                "hold": False,
                "expired": False,
                "data_reclaimed": False,
            }
            obj_meta = self._load_meta(key)
            obj_meta["versions"].append(entry)
            self._save_meta(key, obj_meta)

            # 结算容量：分片计费部分（charged_parts + unused_reserve）已被
            # 「分片数据 + 占座」覆盖，合并产物留下 total，差额即本次净变化。
            self._used_bytes += total - charged_parts - unused_reserve

            meta["completed"] = True
            self._save_upload_meta(upload_id, meta)
            shutil.rmtree(self._upload_dir(upload_id), ignore_errors=True)
            return self._version_from_dict(key, entry)

    def abort_multipart_upload(self, upload_id: str) -> None:
        """中止上传会话，清理已上传的分片，释放占座与分片占用。"""
        with self._lock:
            meta = self._load_upload_meta(upload_id)  # 不存在则抛 NoSuchUpload
            charged_parts = sum(p["size"] for p in meta.get("parts", {}).values())
            reserved = int(meta.get("reserved_bytes", 0))
            unused_reserve = max(0, reserved - charged_parts)
            self._used_bytes -= charged_parts + unused_reserve
            shutil.rmtree(self._upload_dir(upload_id), ignore_errors=True)
