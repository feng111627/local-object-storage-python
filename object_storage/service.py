"""版本化对象服务：对外门面，聚合分片、版本、配额与回收。

版本语义
--------
- 每次覆盖写（put / 分片合并提交）都会在 ``objects/<safe(key).json>``
  中追加一个不可变 :class:`VersionRecord`，并原子切换 "current" 指针；
- 元数据写入走临时文件 + ``os.replace``，配合 key 级互斥锁，读侧要么
  看到旧版本，要么看到完整的新版本，绝不会看到写了一半的内容；
- 删除分为两种：
  * 删除指定版本 -> 该版本打上 ``delete_marker`` 墓碑并释放引用；
  * 删除整个 key（不指定 versionId）-> 追加一个墓碑版本并切换 current。
  读取墓碑版本返回 410 VersionDeleted，读取从不存在的版本返回
  404 NoSuchVersion，读取不存在的 key 返回 404 NoSuchKey，三者可区分。
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import BinaryIO

from .config import StorageConfig
from .gc import GarbageCollector
from .locks import LockRegistry
from .models import PartRecord, UploadRecord, VersionRecord
from .multipart import MultipartManager
from .storage import BlobStore

log = logging.getLogger("object_storage.service")


def _safe_key(key: str) -> str:
    """把 key 转成安全文件名：保留可读字符，其余用 %xx 编码。"""
    out = []
    for ch in key:
        if ch.isalnum() or ch in "._-":
            out.append(ch)
        else:
            out.append("%" + format(ord(ch), "02X"))
    name = "".join(out)
    if len(name) > 200:
        import hashlib

        name = name[:160] + "-" + hashlib.sha256(key.encode()).hexdigest()[:32]
    return name


class _KeyMetadata:
    def __init__(self, key: str, current_version_id: str | None,
                 versions: list[VersionRecord]) -> None:
        self.key = key
        self.current_version_id = current_version_id
        self.versions = versions

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "currentVersionId": self.current_version_id,
            "versions": [v.to_dict() for v in self.versions],
        }

    @classmethod
    def from_dict(cls, data: dict) -> _KeyMetadata:
        return cls(
            key=data["key"],
            current_version_id=data.get("currentVersionId"),
            versions=[VersionRecord.from_dict(v) for v in data.get("versions", [])],
        )

    def find(self, version_id: str) -> VersionRecord | None:
        for v in self.versions:
            if v.version_id == version_id:
                return v
        return None

    @property
    def current(self) -> VersionRecord | None:
        if self.current_version_id is None:
            return None
        return self.find(self.current_version_id)


class ObjectStorageService:
    def __init__(self, data_dir: str, config: StorageConfig | None = None) -> None:
        self.data_dir = data_dir
        self.config = config or StorageConfig()
        self.locks = LockRegistry()
        self.blobs = BlobStore(data_dir)
        self.objects_dir = os.path.join(data_dir, "objects")
        self.uploads_dir = os.path.join(data_dir, "uploads")
        self.multipart = MultipartManager(
            self.blobs, self.locks, self.config, self.uploads_dir
        )
        self.gc = GarbageCollector(
            self.blobs, self.multipart, self.locks, self.config, self.objects_dir
        )
        self._initialized = False

    def initialize(self) -> None:
        os.makedirs(self.objects_dir, exist_ok=True)
        self.blobs.init_dirs()
        self.multipart.init_dirs()
        self.multipart.recover_active_count()
        self._initialized = True
        log.info("object storage initialized at %s", os.path.abspath(self.data_dir))

    def start(self) -> ObjectStorageService:
        if not self._initialized:
            self.initialize()
        if self.config.enable_background_gc and self.config.gc_interval_seconds > 0:
            self.gc.start_background()
        return self

    def shutdown(self) -> None:
        self.gc.stop_background()

    # ---------- key 元数据 ----------
    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str) or not key:
            from .errors import InvalidRequestError

            raise InvalidRequestError(
                "key must be a non-empty string", details={"key": key}
            )

    def _meta_path(self, key: str) -> str:
        return os.path.join(self.objects_dir, _safe_key(key) + ".json")

    def _load_meta_or_none(self, key: str) -> _KeyMetadata | None:
        path = self._meta_path(key)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return _KeyMetadata.from_dict(json.load(f))

    def _load_meta(self, key: str) -> _KeyMetadata:
        meta = self._load_meta_or_none(key)
        if meta is None:
            from .errors import KeyNotFoundError

            raise KeyNotFoundError(f"key not found: {key}", details={"key": key})
        return meta

    def _store_meta(self, meta: _KeyMetadata) -> None:
        path = self._meta_path(meta.key)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(meta.to_dict(), f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _new_version_id(self) -> str:
        # 时间戳前缀使版本天然按时间有序，uuid 保证全局唯一
        return f"{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:12]}"

    def _resolve_for_read(
        self, key: str, version_id: str | None
    ) -> tuple[_KeyMetadata, VersionRecord]:
        meta = self._load_meta_or_none(key)
        from .errors import KeyNotFoundError, VersionDeletedError, VersionNotFoundError

        if meta is None:
            raise KeyNotFoundError(f"key not found: {key}", details={"key": key})
        if version_id is None:
            version = meta.current
            if version is None or version.delete_marker:
                # current 为空或指向墓碑：整个 key 当前不可见，按
                # NoSuchKey(404) 处理（与指定版本读墓碑的 410 区分）。
                raise KeyNotFoundError(f"key has no live version: {key}",
                                       details={"key": key})
        else:
            version = meta.find(version_id)
            if version is None:
                raise VersionNotFoundError(
                    f"version not found: {version_id}",
                    details={"key": key, "versionId": version_id},
                )
        if version.delete_marker:
            raise VersionDeletedError(
                f"version is a delete marker: {version.version_id}",
                details={"key": key, "versionId": version.version_id},
            )
        return meta, version

    # ---------- 简单对象 ----------
    def put_object(
        self,
        key: str,
        stream: BinaryIO,
        checksum: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> VersionRecord:
        self._validate_key(key)
        with self.locks.lock(f"key:{key}"):
            blob_id, size, actual_checksum = self.blobs.put_fileobj(stream, checksum)
            if size > self.config.max_object_size:
                from .errors import ObjectTooLargeError

                raise ObjectTooLargeError(
                    "object exceeds max size",
                    details={"size": size, "limit": self.config.max_object_size},
                )
            return self._commit_version(
                key, blob_id, size, actual_checksum, part_count=1,
                metadata=metadata,
            )

    def _commit_version(
        self,
        key: str,
        blob_id: str,
        size: int,
        checksum: str,
        part_count: int,
        metadata: dict[str, str] | None = None,
    ) -> VersionRecord:
        """调用方必须持有 key 锁。"""
        version = VersionRecord(
            key=key,
            version_id=self._new_version_id(),
            blob_id=blob_id,
            size=size,
            checksum=checksum,
            created_at=time.time(),
            part_count=part_count,
            metadata=dict(metadata or {}),
        )
        meta = self._load_meta_or_none(key)
        if meta is None:
            meta = _KeyMetadata(key, None, [])
        meta.versions.append(version)
        meta.current_version_id = version.version_id
        self._store_meta(meta)  # 原子切换：读侧不会看到半成品
        log.info("committed version key=%s version=%s size=%d parts=%d",
                 key, version.version_id, size, part_count)
        return version

    def get_object(
        self, key: str, version_id: str | None = None
    ) -> tuple[VersionRecord, BinaryIO]:
        # 读路径先在锁内解析版本（current 指针的读取与元数据一致），
        # blob 本身内容寻址且不可变，拿到 blob_id 后无需持锁读取。
        _, version = self._resolve_for_read(key, version_id)
        return version, self.blobs.open_blob(version.blob_id)

    def delete_object(self, key: str, version_id: str | None = None) -> VersionRecord:
        from .errors import KeyNotFoundError, VersionNotFoundError

        with self.locks.lock(f"key:{key}"):
            meta = self._load_meta_or_none(key)
            if meta is None:
                if version_id is not None:
                    raise VersionNotFoundError(
                        f"version not found: {version_id}",
                        details={"key": key, "versionId": version_id},
                    )
                raise KeyNotFoundError(f"key not found: {key}",
                                       details={"key": key})
            if version_id is None:
                # 追加墓碑版本并切换 current
                marker = VersionRecord(
                    key=key,
                    version_id=self._new_version_id(),
                    blob_id="",
                    size=0,
                    checksum="",
                    created_at=time.time(),
                    delete_marker=True,
                )
                meta.versions.append(marker)
                meta.current_version_id = marker.version_id
                self._store_meta(meta)
                log.info("delete marker appended key=%s version=%s", key,
                         marker.version_id)
                return marker
            version = meta.find(version_id)
            if version is None:
                raise VersionNotFoundError(
                    f"version not found: {version_id}",
                    details={"key": key, "versionId": version_id},
                )
            if version.delete_marker:
                # 重复删除同一墓碑版本：返回 410，保持可区分且幂等友好
                from .errors import VersionDeletedError

                raise VersionDeletedError(
                    f"version already deleted: {version_id}",
                    details={"key": key, "versionId": version_id},
                )
            marker = VersionRecord(
                key=key,
                version_id=self._new_version_id(),
                blob_id="",
                size=0,
                checksum="",
                created_at=time.time(),
                delete_marker=True,
                metadata={"deletedVersionId": version.version_id},
            )
            # 指定版本删除：原版本记录保留（带墓碑标记），其 blob 引用
            # 立即释放，由 GC 按引用关系统一回收。之后再读该版本会
            # 命中墓碑，得到 410 VersionDeleted（区别于从未存在的 404）。
            version.delete_marker = True
            version.blob_id = ""
            version.size = 0
            version.checksum = ""
            insert_at = meta.versions.index(version) + 1
            meta.versions.insert(insert_at, marker)
            # 若删的正好是 current，则 current 指向墓碑；
            # 否则 current 不变（其它版本不受影响）。
            if meta.current_version_id == version.version_id:
                meta.current_version_id = marker.version_id
            self._store_meta(meta)
            log.info("deleted version key=%s version=%s marker=%s", key,
                     version_id, marker.version_id)
            return marker

    # ---------- 版本管理 ----------
    def list_versions(self, key: str) -> list[VersionRecord]:
        return self._load_meta(key).versions

    def head_object(self, key: str, version_id: str | None = None) -> VersionRecord:
        _, version = self._resolve_for_read(key, version_id)
        return version

    # ---------- 分片上传 ----------
    def begin_multipart(
        self,
        key: str,
        part_size: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> UploadRecord:
        self._validate_key(key)
        return self.multipart.begin_upload(key, part_size, metadata)

    def upload_part(
        self,
        upload_id: str,
        part_number: int,
        stream: BinaryIO,
        checksum: str | None = None,
    ) -> PartRecord:
        return self.multipart.upload_part(upload_id, part_number, stream, checksum)

    def list_parts(self, upload_id: str) -> list[PartRecord]:
        return self.multipart.list_parts(upload_id)

    def complete_multipart(
        self, upload_id: str, expected_checksum: str | None = None
    ) -> VersionRecord:
        # 先取出会话（确定 key），再在 key 锁内完成"合并 + 提交版本"，
        # 与同一 key 的并发简单写/其它合并写串行化。
        upload = self.multipart.get_upload(upload_id)
        with self.locks.lock(f"key:{upload.key}"):
            # complete_upload 对已完成会话幂等（读回合并 blob 信息，
            # 不重复拼接）。拿到结果后若会话已回填 versionId，说明
            # 版本此前已提交（重复请求 / 崩溃重试），直接返回该版本。
            upload, blob_id, size, checksum = self.multipart.complete_upload(
                upload_id, expected_checksum
            )
            if upload.completed_version_id:
                meta = self._load_meta_or_none(upload.key)
                existing = (
                    meta.find(upload.completed_version_id) if meta else None
                )
                if existing is not None:
                    return existing
            version = self._commit_version(
                upload.key, blob_id, size, checksum,
                part_count=len(upload.parts), metadata=upload.metadata or None,
            )
            self.multipart.attach_version(upload_id, version.version_id)
            self.multipart.finalize_completed(upload_id)
            return version

    def abort_multipart(self, upload_id: str) -> None:
        self.multipart.abort_upload(upload_id)

    def list_uploads(self) -> list[UploadRecord]:
        return self.multipart.list_uploads()

    # ---------- 回收 ----------
    def garbage_collect(self) -> dict:
        return self.gc.sweep()
