"""核心存储：本地磁盘模拟对象存储，支持版本化、分片上传、总容量与读取保护。

一致性设计要点：
- 数据文件先写临时文件再 ``os.replace`` 原子落盘，读者只会看到完整版本；
- 版本一旦写入即不可变；覆盖写生成新版本，删除/淘汰只是打标记，数据文件异步回收。

锁层次（自动维护与正常读写并发的关键）：
- ``_global``：只保护跨 key 的共享状态（用量账本、预留计数），临界区极短；
- 每个 key / 每个上传会话各有一把锁，不同 key 的读写互不阻塞，
  后台回收逐 key 加锁、不会把整个存储卡住；
- 唯一的嵌套顺序是「上传锁 → key 锁」（合并分片时），不存在反向路径，无死锁。

读取保护：
- ``get_object`` 在 key 锁内定位版本并登记 read pin，随后释放 key 锁再读文件；
- 回收数据文件前若发现 pin 未释放，会等待一段时间；仍被读取则本轮跳过、下轮再试，
  因此绝不会删掉正被读取的数据。
"""
from __future__ import annotations

import contextlib
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
    StorageCapacityExceeded,
    TooManyConcurrentUploads,
    TooManyParts,
    UploadAlreadyCompleted,
    VersionAlreadyDeleted,
    VersionExpired,
    VersionProtected,
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
          retention.json                                  # 保留规则
          state/usage.json                                # 用量账本
          state/maintenance.json                          # 自动维护状态
    """

    def __init__(self, root: str | Path, quota: QuotaConfig | None = None, *,
                 reclaim_wait_seconds: float = 2.0):
        self.root = Path(root)
        self.quota = quota or QuotaConfig()
        self.reclaim_wait_seconds = reclaim_wait_seconds
        # 跨 key 共享状态锁
        self._global = threading.RLock()
        # 细粒度资源锁注册表
        self._key_guard = threading.Lock()
        self._key_locks: dict[str, threading.Lock] = {}
        self._upload_guard = threading.Lock()
        self._upload_locks: dict[str, threading.Lock] = {}
        # 读取保护：(key, version_id) -> 正在读取的计数
        self._pin_cond = threading.Condition()
        self._read_pins: dict[tuple[str, str], int] = {}
        # 用量账本（逻辑占用 = 版本数据 + 在传分片）
        self._used_bytes = 0
        self._reserved_bytes = 0
        (self.root / "objects").mkdir(parents=True, exist_ok=True)
        (self.root / "uploads").mkdir(parents=True, exist_ok=True)
        (self.root / "state").mkdir(parents=True, exist_ok=True)
        # 重启后以磁盘实际占用为准重建账本，崩溃/中断也不会算错。
        self._reconcile_usage()

    # ------------------------------------------------------------------
    # 锁与路径辅助
    # ------------------------------------------------------------------
    @contextlib.contextmanager
    def _key_locked(self, key: str):
        with self._key_guard:
            lk = self._key_locks.setdefault(key, threading.Lock())
        lk.acquire()
        try:
            yield
        finally:
            lk.release()

    @contextlib.contextmanager
    def _upload_locked(self, upload_id: str):
        with self._upload_guard:
            lk = self._upload_locks.setdefault(upload_id, threading.Lock())
        lk.acquire()
        try:
            yield
        finally:
            lk.release()

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
            expired=v.get("expired", False),
            frozen=v.get("frozen", False),
            data_reclaimed=v.get("data_reclaimed", False),
        )

    # ------------------------------------------------------------------
    # 用量账本与总容量
    # ------------------------------------------------------------------
    def _usage_path(self) -> Path:
        return self.root / "state" / "usage.json"

    def _save_usage_locked(self) -> None:
        self._atomic_write(self._usage_path(), json.dumps({
            "used_bytes": self._used_bytes,
            "reserved_bytes": self._reserved_bytes,
        }).encode("utf-8"))

    def _reconcile_usage(self) -> None:
        """扫描磁盘重建逻辑占用（版本数据 + 在传分片），重启/崩溃后以此为准。"""
        total = 0
        objects_dir = self.root / "objects"
        if objects_dir.exists():
            for obj_dir in objects_dir.iterdir():
                versions_dir = obj_dir / "versions"
                if not versions_dir.exists():
                    continue
                for f in versions_dir.iterdir():
                    if f.is_file() and not f.name.startswith(".tmp-"):
                        total += f.stat().st_size
        uploads_dir = self.root / "uploads"
        if uploads_dir.exists():
            for up_dir in uploads_dir.iterdir():
                parts_dir = up_dir / "parts"
                if not parts_dir.exists():
                    continue
                for f in parts_dir.iterdir():
                    if f.is_file() and not f.name.startswith(".tmp-"):
                        total += f.stat().st_size
        with self._global:
            self._used_bytes = total
            self._reserved_bytes = 0
            self._save_usage_locked()

    def usage(self) -> dict:
        """当前用量（运维查询）。"""
        with self._global:
            cap = self.quota.max_total_size
            used = self._used_bytes
            reserved = self._reserved_bytes
        return {
            "used_bytes": used,
            "reserved_bytes": reserved,
            "max_total_size": cap,
            "available_bytes": None if cap is None else max(0, cap - used - reserved),
        }

    def _reserve(self, size: int) -> None:
        """预留容量；超限直接拒绝（明确错误，绝不先吃一部分再报错）。"""
        with self._global:
            cap = self.quota.max_total_size
            if cap is not None and self._used_bytes + self._reserved_bytes + size > cap:
                raise StorageCapacityExceeded(
                    f"存储总容量不足：需要 {size} 字节，"
                    f"已用 {self._used_bytes}，预留 {self._reserved_bytes}，上限 {cap}",
                    detail={
                        "need": size,
                        "used_bytes": self._used_bytes,
                        "reserved_bytes": self._reserved_bytes,
                        "max_total_size": cap,
                    },
                )
            self._reserved_bytes += size

    def _release_reservation(self, size: int) -> None:
        with self._global:
            self._reserved_bytes = max(0, self._reserved_bytes - size)
            self._save_usage_locked()

    def _commit_usage(self, size: int) -> None:
        """预留转正：数据已落盘并登记。"""
        with self._global:
            self._used_bytes += size
            self._reserved_bytes = max(0, self._reserved_bytes - size)
            self._save_usage_locked()

    def _release_usage(self, size: int) -> None:
        """数据文件被删除后扣减账本。"""
        with self._global:
            self._used_bytes = max(0, self._used_bytes - size)
            self._save_usage_locked()

    # ------------------------------------------------------------------
    # 读取保护（read pin）
    # ------------------------------------------------------------------
    def _pin(self, key: str, version_id: str) -> None:
        with self._pin_cond:
            k = (key, version_id)
            self._read_pins[k] = self._read_pins.get(k, 0) + 1

    def _unpin(self, key: str, version_id: str) -> None:
        with self._pin_cond:
            k = (key, version_id)
            n = self._read_pins.get(k, 0) - 1
            if n <= 0:
                self._read_pins.pop(k, None)
            else:
                self._read_pins[k] = n
            self._pin_cond.notify_all()

    def is_pinned(self, key: str, version_id: str) -> bool:
        with self._pin_cond:
            return self._read_pins.get((key, version_id), 0) > 0

    def wait_unpinned(self, key: str, version_id: str, *, timeout: float | None = None) -> bool:
        """等待某版本的读取结束；返回是否已无人读取。"""
        deadline = time.monotonic() + (self.reclaim_wait_seconds if timeout is None else timeout)
        with self._pin_cond:
            while self._read_pins.get((key, version_id), 0) > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._pin_cond.wait(remaining)
            return True

    def reclaim_data_file(self, key: str, version_id: str, path: Path) -> int:
        """pin 感知地回收版本数据文件。返回回收字节数（仍被读取则本轮跳过返回 0）。

        调用方必须持有该 key 的锁。
        """
        if not path.exists():
            return 0
        if not self.wait_unpinned(key, version_id):
            return 0
        size = path.stat().st_size
        path.unlink()
        self._release_usage(size)
        return size

    # ------------------------------------------------------------------
    # 简单读写（整体对象）
    # ------------------------------------------------------------------
    def put_object(self, key: str, data: bytes) -> VersionInfo:
        """覆盖写：生成新版本。旧版本保留，直到被删除/淘汰或被 GC 回收。"""
        if len(data) > self.quota.max_object_size:
            raise ObjectTooLarge(
                f"对象大小 {len(data)} 超过上限 {self.quota.max_object_size}",
                detail={"size": len(data), "limit": self.quota.max_object_size},
            )
        size = len(data)
        self._reserve(size)
        try:
            version_id = uuid.uuid4().hex
            version_path = self._object_dir(key) / "versions" / version_id
            with self._key_locked(key):
                self._atomic_write(version_path, data)
                try:
                    meta = self._load_meta(key)
                    entry = {
                        "version_id": version_id,
                        "size": size,
                        "sha256": _sha256(data),
                        "created_at": time.time(),
                        "deleted": False,
                        "delete_marker": False,
                        "expired": False,
                        "frozen": False,
                        "data_reclaimed": False,
                    }
                    meta["versions"].append(entry)
                    self._save_meta(key, meta)
                    self._commit_usage(size)
                except BaseException:
                    version_path.unlink(missing_ok=True)
                    raise
            return self._version_from_dict(key, entry)
        except BaseException:
            self._release_reservation(size)
            raise

    def get_object(self, key: str, version_id: str | None = None) -> tuple[VersionInfo, bytes]:
        """读取对象。不指定版本时读最新的可见版本。

        定位版本时持 key 锁并登记 read pin，真正读文件时释放 key 锁，
        因此大对象读取不会挡住同 key 的维护操作（维护会等读完再回收）。
        """
        with self._key_locked(key):
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
                if entry.get("delete_marker") or entry.get("deleted"):
                    raise VersionAlreadyDeleted(
                        f"版本 {version_id} 已被删除",
                        detail={"key": key, "version_id": version_id},
                    )
                if entry.get("expired"):
                    raise VersionExpired(
                        f"版本 {version_id} 已按保留规则淘汰",
                        detail={"key": key, "version_id": version_id},
                    )
            else:
                # 从最新往回找：删除标记阻断（不会复活旧版本）；跳过删除/淘汰版本。
                entry = None
                for v in reversed(versions):
                    if v.get("delete_marker"):
                        break
                    if not v.get("deleted") and not v.get("expired"):
                        entry = v
                        break
                if entry is None:
                    raise NoSuchKey(f"key {key!r} 不存在或已删除", detail={"key": key})
            data_path = self._object_dir(key) / "versions" / entry["version_id"]
            if not data_path.exists():
                # 元数据可见但数据缺失：属于已被回收的不一致状态。
                if entry.get("expired"):
                    raise VersionExpired(
                        f"版本 {entry['version_id']} 已按保留规则淘汰且数据已回收",
                        detail={"key": key, "version_id": entry["version_id"]},
                    )
                raise VersionAlreadyDeleted(
                    f"版本 {entry['version_id']} 的数据已被回收",
                    detail={"key": key, "version_id": entry["version_id"]},
                )
            self._pin(key, entry["version_id"])
            info = self._version_from_dict(key, entry)
        try:
            return info, data_path.read_bytes()
        finally:
            self._unpin(key, entry["version_id"])

    def delete_object(self, key: str, version_id: str | None = None) -> VersionInfo:
        """删除对象。

        - 指定版本：标记删除；冻结版本受保护，拒绝删除。
        - 不指定版本：写入删除标记，key 逻辑不可见，历史版本仍可按版本号读取。
        """
        with self._key_locked(key):
            meta = self._load_meta(key)
            versions = meta["versions"]
            if version_id is not None:
                entry = next((v for v in versions if v["version_id"] == version_id), None)
                if entry is None:
                    raise NoSuchVersion(
                        f"key {key!r} 没有版本 {version_id}",
                        detail={"key": key, "version_id": version_id},
                    )
                if entry.get("frozen"):
                    raise VersionProtected(
                        f"版本 {version_id} 已被冻结/保留，不能删除",
                        detail={"key": key, "version_id": version_id},
                    )
                if entry.get("deleted") or entry.get("delete_marker"):
                    raise VersionAlreadyDeleted(
                        f"版本 {version_id} 已被删除",
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
                "expired": False,
                "frozen": False,
                "data_reclaimed": True,
            }
            versions.append(marker)
            self._save_meta(key, meta)
            return self._version_from_dict(key, marker)

    def freeze_version(self, key: str, version_id: str) -> VersionInfo:
        """显式冻结/保留某版本：任何自动淘汰与回收都不能移除它。

        数据尚在的已删除/已淘汰版本会因冻结而恢复保留（legal hold 优先）；
        数据已被物理回收、或本身是删除标记，则无法冻结。
        """
        with self._key_locked(key):
            meta = self._load_meta(key)
            entry = next((v for v in meta["versions"] if v["version_id"] == version_id), None)
            if entry is None:
                raise NoSuchVersion(
                    f"key {key!r} 没有版本 {version_id}",
                    detail={"key": key, "version_id": version_id},
                )
            if entry.get("delete_marker"):
                raise VersionProtected(
                    "删除标记没有数据，不能冻结",
                    detail={"key": key, "version_id": version_id},
                )
            data_path = self._object_dir(key) / "versions" / version_id
            if not data_path.exists():
                if entry.get("expired"):
                    raise VersionExpired(
                        "版本数据已被回收，无法冻结",
                        detail={"key": key, "version_id": version_id},
                    )
                raise VersionAlreadyDeleted(
                    "版本数据已被回收，无法冻结",
                    detail={"key": key, "version_id": version_id},
                )
            entry["frozen"] = True
            entry["deleted"] = False
            entry["expired"] = False
            entry["data_reclaimed"] = False
            self._save_meta(key, meta)
            return self._version_from_dict(key, entry)

    def unfreeze_version(self, key: str, version_id: str) -> VersionInfo:
        """解除冻结（不改变其它状态）。"""
        with self._key_locked(key):
            meta = self._load_meta(key)
            entry = next((v for v in meta["versions"] if v["version_id"] == version_id), None)
            if entry is None:
                raise NoSuchVersion(
                    f"key {key!r} 没有版本 {version_id}",
                    detail={"key": key, "version_id": version_id},
                )
            entry["frozen"] = False
            self._save_meta(key, meta)
            return self._version_from_dict(key, entry)

    def list_versions(self, key: str) -> list[VersionInfo]:
        """列出版本，最新在前（含删除标记、已删除、已淘汰与冻结版本）。"""
        with self._key_locked(key):
            meta = self._load_meta(key)
            if not meta["versions"] and not self._object_dir(key).exists():
                raise NoSuchKey(f"key {key!r} 不存在", detail={"key": key})
            return [self._version_from_dict(key, v) for v in reversed(meta["versions"])]

    def iter_object_keys(self) -> list[str]:
        """列出磁盘上所有存有元数据的 key（维护扫描用）。"""
        objects_dir = self.root / "objects"
        keys: list[str] = []
        if not objects_dir.exists():
            return keys
        for obj_dir in objects_dir.iterdir():
            if not obj_dir.is_dir() or not (obj_dir / "meta.json").exists():
                continue
            try:
                meta = json.loads((obj_dir / "meta.json").read_text(encoding="utf-8"))
                keys.append(meta["key"])
            except (json.JSONDecodeError, KeyError):
                # 目录名也可还原 key，保证损坏的 meta 不阻断扫描
                keys.append(unquote(obj_dir.name))
        return keys

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
        with self._global:
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
            session_meta = {
                "upload_id": upload_id,
                "key": key,
                "created_at": time.time(),
                "expected_size": expected_size,
                "expected_sha256": expected_sha256,
                "parts": {},
                "completed": False,
            }
            self._save_upload_meta(upload_id, session_meta)
        (self._upload_dir(upload_id) / "parts").mkdir(parents=True, exist_ok=True)
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
        size = len(data)
        with self._upload_locked(upload_id):
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
            total = sum(p["size"] for p in parts.values()) + size
            if total > self.quota.max_object_size:
                raise ObjectTooLarge(
                    f"累计分片大小 {total} 超过对象上限 {self.quota.max_object_size}",
                    detail={"size": total, "limit": self.quota.max_object_size},
                )
            self._reserve(size)  # 总容量不足：明确拒绝，分片不落盘
            try:
                self._atomic_write(self._upload_dir(upload_id) / "parts" / str(part_number), data)
                parts[key] = {"size": size, "sha256": digest, "uploaded_at": time.time()}
                self._save_upload_meta(upload_id, meta)
                self._commit_usage(size)
            except BaseException:
                (self._upload_dir(upload_id) / "parts" / str(part_number)).unlink(missing_ok=True)
                self._release_reservation(size)
                raise
            p = parts[key]
            return PartInfo(part_number=part_number, size=p["size"], sha256=p["sha256"], uploaded_at=p["uploaded_at"])

    def list_parts(self, upload_id: str) -> list[PartInfo]:
        """查询已上传的分片清单（断点续传时用于确认进度）。"""
        with self._upload_locked(upload_id):
            meta = self._load_upload_meta(upload_id)
            return [
                PartInfo(part_number=int(num), size=p["size"], sha256=p["sha256"], uploaded_at=p["uploaded_at"])
                for num, p in sorted(meta["parts"].items(), key=lambda kv: int(kv[0]))
            ]

    def complete_multipart_upload(self, upload_id: str, parts: list[int] | None = None) -> VersionInfo:
        """合并分片生成新版本。

        - 清单引用了未上传的分片 → MissingParts；
        - 合并结果与声明的大小/校验不一致 → CompleteChecksumMismatch；
        - 合并后总占用超总容量 → StorageCapacityExceeded；
        - 重复合并同一会话 → UploadAlreadyCompleted（不会生成第二个版本）。
        """
        with self._upload_locked(upload_id):
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

            parts_total = sum(available[n]["size"] for n in wanted)
            all_parts_size = sum(p["size"] for p in available.values())
            key = meta["key"]

            # 产物就是所选分片的拼接，合并成功后整个会话目录（含未选用分片）被删除：
            # 最终占用 = 当前已用 + 在途预留 - 本会话全部分片 + 产物。
            # 按这个「最终占用」明确判定；在途写入都已计入 reserved，判定是保守的。
            with self._global:
                cap = self.quota.max_total_size
                projected = self._used_bytes + self._reserved_bytes - all_parts_size + parts_total
                if cap is not None and projected > cap:
                    raise StorageCapacityExceeded(
                        f"合并后总占用 {projected} 将超过总容量上限 {cap}",
                        detail={
                            "projected_bytes": projected,
                            "max_total_size": cap,
                            "upload_id": upload_id,
                        },
                    )

            version_id = uuid.uuid4().hex
            version_path = self._object_dir(key) / "versions" / version_id
            version_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with self._key_locked(key):
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
                        "expired": False,
                        "frozen": False,
                        "data_reclaimed": False,
                    }
                    obj_meta = self._load_meta(key)
                    obj_meta["versions"].append(entry)
                    self._save_meta(key, obj_meta)
                    # 产物入账
                    with self._global:
                        self._used_bytes += total
                        self._save_usage_locked()

                meta["completed"] = True
                self._save_upload_meta(upload_id, meta)
            except BaseException:
                version_path.unlink(missing_ok=True)
                raise

            # 合并成功后销毁整个会话目录（含未选用分片）并扣减其占用。
            shutil.rmtree(self._upload_dir(upload_id), ignore_errors=True)
            if all_parts_size:
                self._release_usage(all_parts_size)
            return self._version_from_dict(key, entry)

    def abort_multipart_upload(self, upload_id: str) -> None:
        """中止上传会话，清理已上传的分片并释放占用。"""
        with self._upload_locked(upload_id):
            meta = self._load_upload_meta(upload_id)  # 不存在则抛 NoSuchUpload
            removed = sum(p["size"] for p in meta.get("parts", {}).values())
            shutil.rmtree(self._upload_dir(upload_id), ignore_errors=True)
            if removed:
                self._release_usage(removed)
