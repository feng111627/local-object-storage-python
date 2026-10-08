"""错误分类：所有业务错误都带稳定的 error code，便于上层区分处理。"""
from __future__ import annotations


class StorageError(Exception):
    """所有存储层错误的基类。"""

    code: str = "storage_error"
    http_status: int = 500

    def __init__(self, message: str = "", *, detail: dict | None = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.detail = detail or {}

    def to_dict(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "detail": self.detail}}


class NoSuchKey(StorageError):
    code = "no_such_key"
    http_status = 404


class NoSuchVersion(StorageError):
    code = "no_such_version"
    http_status = 404


class VersionAlreadyDeleted(StorageError):
    code = "version_already_deleted"
    http_status = 410


class VersionExpired(StorageError):
    """版本存在，但已被保留规则淘汰回收（与"本来就没这个版本"明确区分）。"""
    code = "version_expired"
    http_status = 410


class VersionOnHold(StorageError):
    """版本被显式冻结/保留，不能删除。"""
    code = "version_on_hold"
    http_status = 409


class NoSuchUpload(StorageError):
    code = "no_such_upload"
    http_status = 404


class UploadAlreadyCompleted(StorageError):
    code = "upload_already_completed"
    http_status = 409


class PartConflict(StorageError):
    """同一分片被并发/重复上传且内容不一致。"""
    code = "part_conflict"
    http_status = 409


class PartChecksumMismatch(StorageError):
    code = "part_checksum_mismatch"
    http_status = 400


class MissingParts(StorageError):
    """合并时清单里引用了不存在的分片。"""
    code = "missing_parts"
    http_status = 400


class CompleteChecksumMismatch(StorageError):
    """合并后整体校验不通过。"""
    code = "complete_checksum_mismatch"
    http_status = 400


class QuotaExceeded(StorageError):
    code = "quota_exceeded"
    http_status = 413


class TooManyConcurrentUploads(QuotaExceeded):
    code = "too_many_concurrent_uploads"
    http_status = 429


class ObjectTooLarge(QuotaExceeded):
    code = "object_too_large"
    http_status = 413


class TooManyParts(QuotaExceeded):
    code = "too_many_parts"
    http_status = 413


class TotalCapacityExceeded(QuotaExceeded):
    """整个存储的总容量超限。

    detail.scope 标明在什么环节被拒：put_object / new_upload / upload_part /
    complete_upload；detail.used/limit/requested 给出容量判定依据。
    """
    code = "total_capacity_exceeded"
    http_status = 507


class RetentionRuleInvalid(StorageError):
    """保留规则参数非法（如 max_versions <= 0）。"""
    code = "retention_rule_invalid"
    http_status = 400
