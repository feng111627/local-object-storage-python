"""错误分类体系。

所有对外错误都继承 :class:`StorageError`，带有稳定的机器可读 ``code``
与建议的 HTTP ``status_code``，上层可据此区分不同的失败原因。
"""

from __future__ import annotations


class StorageError(Exception):
    """所有存储错误的基类。"""

    code = "InternalError"
    status_code = 500

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class KeyNotFoundError(StorageError):
    code = "NoSuchKey"
    status_code = 404


class VersionNotFoundError(StorageError):
    """指定的 versionId 从未存在，或其所属 key 不存在。"""

    code = "NoSuchVersion"
    status_code = 404


class VersionDeletedError(StorageError):
    """版本曾经存在，但已经被删除（墓碑）。"""

    code = "VersionDeleted"
    status_code = 410


class UploadNotFoundError(StorageError):
    code = "NoSuchUpload"
    status_code = 404


class UploadAlreadyCompletedError(StorageError):
    code = "UploadAlreadyCompleted"
    status_code = 409


class MissingPartError(StorageError):
    """合并时分片编号不连续、存在缺口。"""

    code = "MissingPart"
    status_code = 400


class PartTooSmallError(StorageError):
    """除最后一个分片外，分片大小低于初始化时声明的 partSize。"""

    code = "EntityTooSmall"
    status_code = 400


class PartChecksumMismatchError(StorageError):
    """上传分片时客户端声明的校验和与实际字节不一致。"""

    code = "BadDigest"
    status_code = 400


class ObjectChecksumMismatchError(StorageError):
    """合并后整对象校验和与客户端声明不一致，拒绝提交版本。"""

    code = "InvalidObjectChecksum"
    status_code = 400


class ObjectTooLargeError(StorageError):
    code = "EntityTooLarge"
    status_code = 413


class TooManyPartsError(StorageError):
    code = "TooManyParts"
    status_code = 413


class TooManyUploadsError(StorageError):
    code = "SlowDown"
    status_code = 429


class InvalidPartNumberError(StorageError):
    code = "InvalidPartNumber"
    status_code = 400


class InvalidRequestError(StorageError):
    """请求体 / 参数本身不合法（如空 key、body 非 JSON）。"""

    code = "InvalidRequest"
    status_code = 400


class StorageCorruptionError(StorageError):
    """元数据引用的 blob 在磁盘上缺失或损坏。"""

    code = "StorageCorruption"
    status_code = 500
