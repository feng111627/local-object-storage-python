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
    UploadAlreadyCompleted,
    VersionAlreadyDeleted,
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
        (self.root / "objects").mkdir(parents=True, exist_ok=True)
        (self.root / "uploads").mkdir(parents=True, exist_ok=True)

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
            version_id = uuid.uuid4().hex
            self._atomic_write(self._object_dir(key) / "versions" / version_id, data)
            meta = self._load_meta(key)
            entry = {
                "version_id": version_id,
                "size": len(data),
                "sha256": digest,
                "created_at": time.time(),
                "deleted": False,
                "delete_marker": False,
            }
            meta["versions"].append(entry)
            self._save_meta(key, meta)
            return self._version_from_dict(key, entry)

    def get_object(self, key: str, version_id: str | None = None) -> tuple[VersionInfo, bytes]:
        """读取对象。不指定版本时读最新的可见版本。"""
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
                if entry.get("deleted") or entry.get("delete_marker"):
                    raise VersionAlreadyDeleted(
                        f"版本 {version_id} 已被删除",
                        detail={"key": key, "version_id": version_id},
                    )
            else:
                # 从最新往回找：遇到删除标记则 key 逻辑删除（不会复活旧版本）；
                # 跳过被单独删除的版本，落在最近的可见版本上。
                entry = None
                for v in reversed(versions):
                    if v.get("delete_marker"):
                        break
                    if not v.get("deleted"):
                        entry = v
                        break
                if entry is None:
                    raise NoSuchKey(f"key {key!r} 已删除", detail={"key": key})
            data_path = self._object_dir(key) / "versions" / entry["version_id"]
            if not data_path.exists():
                # 数据已被 GC 回收但元数据标记为可见：属于内部不一致，按已删除处理。
                raise VersionAlreadyDeleted(
                    f"版本 {entry['version_id']} 的数据已被回收",
                    detail={"key": key, "version_id": entry["version_id"]},
                )
            return self._version_from_dict(key, entry), data_path.read_bytes()

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
            self._atomic_write(self._upload_dir(upload_id) / "parts" / str(part_number), data)
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
            }
            obj_meta = self._load_meta(key)
            obj_meta["versions"].append(entry)
            self._save_meta(key, obj_meta)

            meta["completed"] = True
            self._save_upload_meta(upload_id, meta)
            shutil.rmtree(self._upload_dir(upload_id), ignore_errors=True)
            return self._version_from_dict(key, entry)

    def abort_multipart_upload(self, upload_id: str) -> None:
        """中止上传会话，清理已上传的分片。"""
        with self._lock:
            self._load_upload_meta(upload_id)  # 不存在则抛 NoSuchUpload
            shutil.rmtree(self._upload_dir(upload_id), ignore_errors=True)
