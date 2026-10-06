"""数据模型：版本、分片、上传会话、配额配置。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True)
class QuotaConfig:
    """配额限制。"""
    max_object_size: int = 64 * 1024 * 1024      # 单对象整体大小上限（字节）
    max_parts_per_upload: int = 1000             # 单次分片上传的分片数上限
    max_concurrent_uploads: int = 8              # 同一 key 的并发上传会话上限
    max_part_size: int = 16 * 1024 * 1024        # 单个分片大小上限


@dataclass
class PartInfo:
    part_number: int
    size: int
    sha256: str
    uploaded_at: float = field(default_factory=time.time)


@dataclass
class VersionInfo:
    version_id: str
    key: str
    size: int
    sha256: str
    created_at: float
    deleted: bool = False
    delete_marker: bool = False


@dataclass
class UploadSession:
    upload_id: str
    key: str
    created_at: float
    expected_size: int | None = None
    expected_sha256: str | None = None
    parts: dict[int, PartInfo] = field(default_factory=dict)
    completed: bool = False
