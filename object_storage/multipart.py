"""分片上传会话管理：初始化、幂等上传分片、清单查询、合并、中止。

会话元数据以 JSON 文件持久化（``uploads/<upload_id>.json``），写入走
临时文件 + ``os.replace`` 原子替换，因此：

- 进程异常中断后，已上传的分片与清单仍然完整，可断点续传；
- 并发上传同一分片时，通过会话锁串行化，同内容重复上传直接返回既有
  记录（幂等），不同内容重复上传同一编号会被明确拒绝（首写为准）；
- 合并前做存在性、连续性、大小、数量、整对象校验和多重校验，任一
  不通过都不会产生新版本。
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from typing import BinaryIO

from .config import StorageConfig
from .errors import (
    InvalidPartNumberError,
    MissingPartError,
    ObjectChecksumMismatchError,
    ObjectTooLargeError,
    PartTooSmallError,
    TooManyPartsError,
    TooManyUploadsError,
    UploadAlreadyCompletedError,
    UploadNotFoundError,
)
from .locks import LockRegistry
from .models import PartRecord, UploadRecord
from .storage import BlobStore


class MultipartManager:
    def __init__(
        self,
        blob_store: BlobStore,
        locks: LockRegistry,
        config: StorageConfig,
        uploads_dir: str,
    ) -> None:
        self.blobs = blob_store
        self.locks = locks
        self.config = config
        self.uploads_dir = uploads_dir
        self._active = 0
        self._active_guard = threading.Lock()

    def init_dirs(self) -> None:
        os.makedirs(self.uploads_dir, exist_ok=True)
        self.blobs.init_dirs()

    def recover_active_count(self) -> int:
        """进程重启后，按磁盘上的未完成会话文件重建并发槽计数。"""
        count = 0
        for upload in self.list_uploads():
            if not upload.completed:
                count += 1
        with self._active_guard:
            self._active = count
        return count

    # ---- 元数据持久化 ----
    def _path(self, upload_id: str) -> str:
        return os.path.join(self.uploads_dir, f"{upload_id}.json")

    def _load(self, upload_id: str) -> UploadRecord:
        path = self._path(upload_id)
        if not os.path.exists(path):
            raise UploadNotFoundError(
                f"upload not found: {upload_id}",
                details={"uploadId": upload_id},
            )
        with open(path, "r", encoding="utf-8") as f:
            return UploadRecord.from_dict(json.load(f))

    def _store(self, upload: UploadRecord) -> None:
        tmp_path = os.path.join(self.uploads_dir, f".{upload.upload_id}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(upload.to_dict(), f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self._path(upload.upload_id))

    def _remove_file(self, upload_id: str) -> None:
        try:
            os.remove(self._path(upload_id))
        except FileNotFoundError:
            pass

    # ---- 配额计数 ----
    def _acquire_active_slot(self) -> None:
        with self._active_guard:
            if self._active >= self.config.max_concurrent_uploads:
                raise TooManyUploadsError(
                    "concurrent multipart upload limit reached",
                    details={
                        "active": self._active,
                        "limit": self.config.max_concurrent_uploads,
                    },
                )
            self._active += 1

    def release_active_slot(self) -> None:
        with self._active_guard:
            if self._active > 0:
                self._active -= 1

    @property
    def active_upload_count(self) -> int:
        return self._active

    # ---- 对外操作 ----
    def begin_upload(
        self,
        key: str,
        part_size: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> UploadRecord:
        part_size = part_size or self.config.min_part_size
        if part_size <= 0:
            raise InvalidPartNumberError(
                "partSize must be positive", details={"partSize": part_size}
            )
        self._acquire_active_slot()
        upload = UploadRecord(
            upload_id=uuid.uuid4().hex,
            key=key,
            part_size=part_size,
            created_at=time.time(),
            metadata=dict(metadata or {}),
        )
        try:
            self._store(upload)
        except BaseException:
            self.release_active_slot()
            raise
        return upload

    def upload_part(
        self,
        upload_id: str,
        part_number: int,
        stream: BinaryIO,
        checksum: str | None = None,
    ) -> PartRecord:
        if not isinstance(part_number, int) or not (
            1 <= part_number <= self.config.max_parts_per_upload
        ):
            raise InvalidPartNumberError(
                f"part number must be in [1, {self.config.max_parts_per_upload}]",
                details={"partNumber": part_number},
            )
        with self.locks.lock(f"upload:{upload_id}"):
            upload = self._load(upload_id)
            if upload.completed:
                raise UploadAlreadyCompletedError(
                    f"upload already completed: {upload_id}",
                    details={"uploadId": upload_id,
                             "versionId": upload.completed_version_id},
                )
            existing = upload.parts.get(part_number)
            # 内容寻址：写入（含服务端校验和复算）。坏校验会在此处被拒。
            blob_id, size, actual_checksum = self.blobs.put_fileobj(stream, checksum)
            if existing is not None:
                # 幂等：同内容重传（或并发重复传）直接返回既有记录。
                if existing.blob_id == blob_id:
                    return existing
                # 同编号不同内容：首写为准，明确拒绝而不是静默覆盖。
                from .errors import PartChecksumMismatchError

                raise PartChecksumMismatchError(
                    f"part {part_number} already uploaded with different content",
                    details={
                        "partNumber": part_number,
                        "existingChecksum": existing.checksum,
                        "newChecksum": actual_checksum,
                    },
                )
            # 分片数量上限
            if len(upload.parts) + 1 > self.config.max_parts_per_upload:
                raise TooManyPartsError(
                    "too many parts",
                    details={"limit": self.config.max_parts_per_upload},
                )
            # 整体大小上限（按已上传分片累计预估）
            total = sum(p.size for p in upload.parts.values()) + size
            if total > self.config.max_object_size:
                raise ObjectTooLargeError(
                    "object exceeds max size",
                    details={"total": total, "limit": self.config.max_object_size},
                )
            record = PartRecord(
                part_number=part_number,
                blob_id=blob_id,
                size=size,
                checksum=actual_checksum,
                etag=actual_checksum,
                uploaded_at=time.time(),
            )
            upload.parts[part_number] = record
            self._store(upload)
            return record

    def list_parts(self, upload_id: str) -> list[PartRecord]:
        with self.locks.lock(f"upload:{upload_id}"):
            upload = self._load(upload_id)
            return [upload.parts[n] for n in sorted(upload.parts)]

    def get_upload(self, upload_id: str) -> UploadRecord:
        with self.locks.lock(f"upload:{upload_id}"):
            return self._load(upload_id)

    def complete_upload(
        self,
        upload_id: str,
        expected_checksum: str | None = None,
    ) -> tuple[UploadRecord, str, int, str]:
        """校验并合并，返回 (upload 记录, 合并后 blob_id, size, checksum)。"""
        with self.locks.lock(f"upload:{upload_id}"):
            upload = self._load(upload_id)
            if upload.completed:
                # 重复合并是幂等的：直接返回已合并结果，而不是再拼一次。
                meta = self._read_completed_meta(upload_id)
                return upload, meta["blobId"], meta["size"], meta["checksum"]

            if not upload.parts:
                raise MissingPartError(
                    "no parts uploaded", details={"uploadId": upload_id}
                )
            numbers = sorted(upload.parts)
            # 1) 连续性：必须从 1 开始且无缺口
            expected = list(range(1, numbers[-1] + 1))
            if numbers != expected:
                missing = sorted(set(expected) - set(numbers))
                raise MissingPartError(
                    f"missing parts: {missing}",
                    details={"missing": missing, "uploadId": upload_id},
                )
            # 2) 非末片大小
            for n in numbers[:-1]:
                part = upload.parts[n]
                if part.size < upload.part_size:
                    raise PartTooSmallError(
                        f"part {n} size {part.size} < partSize {upload.part_size}",
                        details={
                            "partNumber": n,
                            "size": part.size,
                            "partSize": upload.part_size,
                        },
                    )
            # 3) 数量与总大小
            if len(numbers) > self.config.max_parts_per_upload:
                raise TooManyPartsError(
                    "too many parts",
                    details={"count": len(numbers),
                             "limit": self.config.max_parts_per_upload},
                )
            total = sum(upload.parts[n].size for n in numbers)
            if total > self.config.max_object_size:
                raise ObjectTooLargeError(
                    "object exceeds max size",
                    details={"size": total, "limit": self.config.max_object_size},
                )
            # 4) 合并（合并过程会校验每个分片 blob 都在磁盘上）
            blob_ids = [upload.parts[n].blob_id for n in numbers]
            merged_blob_id, merged_size, merged_checksum = self.blobs.concatenate(
                blob_ids
            )
            if expected_checksum is not None and (
                merged_checksum != expected_checksum.lower()
            ):
                raise ObjectChecksumMismatchError(
                    "merged object checksum mismatch",
                    details={
                        "expected": expected_checksum,
                        "actual": merged_checksum,
                    },
                )
            # 5) 原子地标记完成（先落 completed 元数据，再翻转 upload 状态）
            completed_meta_path = os.path.join(
                self.uploads_dir, f"{upload_id}.completed.json"
            )
            tmp = completed_meta_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "blobId": merged_blob_id,
                        "size": merged_size,
                        "checksum": merged_checksum,
                        "partCount": len(numbers),
                    },
                    f,
                )
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, completed_meta_path)

            upload.completed = True
            upload.metadata["_completed_checksum"] = merged_checksum
            self._store(upload)
            return upload, merged_blob_id, merged_size, merged_checksum

    def attach_version(self, upload_id: str, version_id: str) -> None:
        """版本提交成功后回填 versionId（service 在 key 锁内调用）。"""
        with self.locks.lock(f"upload:{upload_id}"):
            upload = self._load(upload_id)
            if not upload.completed:
                raise UploadNotFoundError(
                    f"upload not completed: {upload_id}",
                    details={"uploadId": upload_id},
                )
            upload.completed_version_id = version_id
            self._store(upload)

    def finalize_completed(self, upload_id: str) -> None:
        """版本已提交后释放并发槽。会话文件保留（标记 completed 并带有
        versionId），使重复合并请求在会话保留期内幂等；会话文件最终由
        GC 回收。"""
        with self.locks.lock(f"upload:{upload_id}"):
            self.release_active_slot()

    def _read_completed_meta(self, upload_id: str) -> dict:
        completed_meta_path = os.path.join(
            self.uploads_dir, f"{upload_id}.completed.json"
        )
        with open(completed_meta_path, encoding="utf-8") as f:
            return json.load(f)

    def abort_upload(self, upload_id: str) -> None:
        with self.locks.lock(f"upload:{upload_id}"):
            upload = self._load(upload_id)
            if upload.completed:
                raise UploadAlreadyCompletedError(
                    f"upload already completed: {upload_id}",
                    details={"uploadId": upload_id},
                )
            self._remove_file(upload_id)
            self.release_active_slot()
            # 分片 blob 不立即删除：由 GC 按引用关系统一回收，
            # 避免误删可能正被其它会话/版本共享的内容寻址 blob。

    def list_uploads(self, include_completed: bool = False) -> list[UploadRecord]:
        out: list[UploadRecord] = []
        for name in os.listdir(self.uploads_dir):
            if (
                name.endswith(".json")
                and not name.startswith(".")
                and not name.endswith(".completed.json")
            ):
                with open(os.path.join(self.uploads_dir, name), encoding="utf-8") as f:
                    upload = UploadRecord.from_dict(json.load(f))
                if include_completed or not upload.completed:
                    out.append(upload)
        out.sort(key=lambda u: u.created_at)
        return out

    def iter_expired_uploads(self, now: float | None = None) -> list[UploadRecord]:
        """列出可由 GC 清理的会话：
        - 未完成且已超 TTL 的上传（需中止）；
        - 已完成并回填版本、保留期已过的会话（删除文件，blob 引用不受
          影响，因为版本元数据本身已持有 blobId）。
        """
        now = time.time() if now is None else now
        result: list[UploadRecord] = []
        for upload in self.list_uploads(include_completed=True):
            age = now - upload.created_at
            if upload.completed:
                # 已完成会话给一个与 TTL 等长的幂等保护窗口，窗口过后清理。
                if age > self.config.multipart_ttl_seconds:
                    result.append(upload)
            elif age > self.config.multipart_ttl_seconds:
                result.append(upload)
        return result

    def delete_session_files(self, upload_id: str) -> None:
        """物理删除会话与其 completed 辅助文件（GC 用）。"""
        with self.locks.lock(f"upload:{upload_id}"):
            self._remove_file(upload_id)
            completed_meta_path = os.path.join(
                self.uploads_dir, f"{upload_id}.completed.json"
            )
            try:
                os.remove(completed_meta_path)
            except FileNotFoundError:
                pass
