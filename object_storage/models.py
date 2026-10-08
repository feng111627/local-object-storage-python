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
    max_total_size: int | None = None            # 整个存储的总容量上限（字节，None 不限）


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
    expired: bool = False        # 被保留规则淘汰（元数据保留，数据由回收清理）
    frozen: bool = False         # 显式冻结/保留：任何自动维护都不能移除
    data_reclaimed: bool = False # 数据文件已被回收


@dataclass(frozen=True)
class RetentionRule:
    """版本保留规则。

    - scope="key"：精确匹配某个 key；scope="prefix"：前缀匹配；
    - max_versions：最多保留多少个存活版本（当前可见版本与冻结版本始终保留）；
    - max_age_seconds：版本最长存活时间（当前可见版本与冻结版本不受年龄淘汰）。
    """
    id: str
    scope: str
    match: str
    max_versions: int | None = None
    max_age_seconds: float | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class UploadSession:
    upload_id: str
    key: str
    created_at: float
    expected_size: int | None = None
    expected_sha256: str | None = None
    parts: dict[int, PartInfo] = field(default_factory=dict)
    completed: bool = False
