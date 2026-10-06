"""内容寻址的磁盘 blob 存储。

数据字节以其 sha256 命名落盘（``blobs/ab/abcd...``），天然去重；
写入通过 ``tmp`` 目录临时文件 + ``os.replace`` 原子完成，读侧永远
只会看到完整文件，绝不会读到写了一半的内容。
"""

from __future__ import annotations

import hashlib
import os
import uuid
from typing import BinaryIO

from .errors import PartChecksumMismatchError, StorageCorruptionError

_CHUNK = 1024 * 1024


class BlobStore:
    def __init__(self, root: str) -> None:
        self.root = root
        self.blobs_dir = os.path.join(root, "blobs")
        self.tmp_dir = os.path.join(root, "tmp")

    def init_dirs(self) -> None:
        os.makedirs(self.blobs_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)

    @staticmethod
    def _path(blobs_dir: str, blob_id: str) -> str:
        return os.path.join(blobs_dir, blob_id[:2], blob_id)

    def blob_path(self, blob_id: str) -> str:
        return self._path(self.blobs_dir, blob_id)

    def put_fileobj(
        self,
        stream: BinaryIO,
        expected_checksum: str | None = None,
    ) -> tuple[str, int, str]:
        """流式写入一个 blob，返回 (blob_id, size, checksum)。

        若提供 ``expected_checksum``（sha256 hex），先在临时文件中写完
        并复算；不一致则删除临时文件并抛 BadDigest，脏数据绝不进入
        blobs 目录。
        """
        self.init_dirs()
        tmp_path = os.path.join(self.tmp_dir, f"put-{uuid.uuid4().hex}")
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp_path, "wb") as out:
                while True:
                    chunk = stream.read(_CHUNK)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    size += len(chunk)
                    out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            self._safe_unlink(tmp_path)
            raise
        checksum = hasher.hexdigest()
        if expected_checksum is not None and checksum != expected_checksum.lower():
            self._safe_unlink(tmp_path)
            raise PartChecksumMismatchError(
                f"checksum mismatch: expected {expected_checksum}, got {checksum}",
                details={"expected": expected_checksum, "actual": checksum},
            )
        final = self._path(self.blobs_dir, checksum)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        try:
            os.replace(tmp_path, final)  # 与已存在的同内容文件并发替换也安全
        except OSError:
            self._safe_unlink(tmp_path)
            if not os.path.exists(final):
                raise
        return checksum, size, checksum

    def put_bytes(
        self, data: bytes, expected_checksum: str | None = None
    ) -> tuple[str, int, str]:
        return self.put_fileobj(io_BytesIO(data), expected_checksum)

    def open_blob(self, blob_id: str) -> BinaryIO:
        path = self.blob_path(blob_id)
        if not os.path.exists(path):
            raise StorageCorruptionError(
                f"blob missing on disk: {blob_id}",
                details={"blobId": blob_id, "path": path},
            )
        return open(path, "rb")

    def exists(self, blob_id: str) -> bool:
        return os.path.exists(self.blob_path(blob_id))

    def verify_blob(self, blob_id: str, expected_checksum: str | None = None) -> str:
        """读取整个 blob 复算校验和；缺文件抛 StorageCorruptionError，
        与期望不符抛 StorageCorruptionError。返回实际 checksum。"""
        path = self.blob_path(blob_id)
        if not os.path.exists(path):
            raise StorageCorruptionError(
                f"blob missing on disk: {blob_id}",
                details={"blobId": blob_id},
            )
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
        actual = hasher.hexdigest()
        if expected_checksum is not None and actual != expected_checksum.lower():
            raise StorageCorruptionError(
                f"blob corrupted: {blob_id}",
                details={"blobId": blob_id, "expected": expected_checksum,
                         "actual": actual},
            )
        return actual

    def delete_blob(self, blob_id: str) -> bool:
        path = self.blob_path(blob_id)
        try:
            os.remove(path)
        except FileNotFoundError:
            return False
        # 尽力清理空的分片目录
        shard_dir = os.path.dirname(path)
        try:
            os.rmdir(shard_dir)
        except OSError:
            pass
        return True

    def concatenate(self, blob_ids: list[str]) -> tuple[str, int, str]:
        """把多个 blob 顺序合并为一个新 blob，返回 (blob_id, size, checksum)。

        合并期间每个输入 blob 都会被校验存在性；输出走临时文件原子
        替换，任何中断都不会留下半成品 blob。
        """
        self.init_dirs()
        tmp_path = os.path.join(self.tmp_dir, f"concat-{uuid.uuid4().hex}")
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp_path, "wb") as out:
                for blob_id in blob_ids:
                    path = self.blob_path(blob_id)
                    if not os.path.exists(path):
                        raise StorageCorruptionError(
                            f"part blob missing on disk: {blob_id}",
                            details={"blobId": blob_id},
                        )
                    with open(path, "rb") as part:
                        while True:
                            chunk = part.read(_CHUNK)
                            if not chunk:
                                break
                            hasher.update(chunk)
                            size += len(chunk)
                            out.write(chunk)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            self._safe_unlink(tmp_path)
            raise
        checksum = hasher.hexdigest()
        final = self._path(self.blobs_dir, checksum)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        try:
            os.replace(tmp_path, final)
        except OSError:
            self._safe_unlink(tmp_path)
            if not os.path.exists(final):
                raise
        return checksum, size, checksum

    @staticmethod
    def checksum_of_fileobj(stream: BinaryIO) -> tuple[str, int]:
        hasher = hashlib.sha256()
        size = 0
        while True:
            chunk = stream.read(_CHUNK)
            if not chunk:
                break
            hasher.update(chunk)
            size += len(chunk)
        return hasher.hexdigest(), size

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError:
            pass


def io_BytesIO(data: bytes) -> BinaryIO:
    import io

    return io.BytesIO(data)
