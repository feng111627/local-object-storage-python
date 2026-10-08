"""数据模型：版本、分片、上传会话、配额配置、保留规则。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class QuotaConfig:
    """配额限制。"""
    max_object_size: int = 64 * 1024 * 1024      # 单对象整体大小上限（字节）
    max_parts_per_upload: int = 1000             # 单次分片上传的分片数上限
    max_concurrent_uploads: int = 8              # 同一 key 的并发上传会话上限
    max_part_size: int = 16 * 1024 * 1024        # 单个分片大小上限
    max_total_bytes: int | None = None           # 整个存储的总容量上限（None 表示不限）


@dataclass(frozen=True)
class RetentionRule:
    """版本保留规则。

    - scope="key"：精确匹配某个 key；scope="prefix"：按前缀匹配。
    - max_versions：最多保留多少个（非删除标记的）版本，超出部分的旧版本可被淘汰；
    - max_age_seconds：版本最长保留时长，超龄版本可被淘汰；
    - 两者为 None 表示该维度不限制；同时配置时任一命中即满足淘汰条件。
    - 当前可见版本、被显式冻结（hold）的版本、正在被读取的版本，即使命中也不淘汰。
    """
    scope: Literal["key", "prefix"]
    pattern: str
    max_versions: int | None = None
    max_age_seconds: float | None = None
    updated_at: float = field(default_factory=time.time)


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
    data_reclaimed: bool = False   # 数据文件已被回收（删除/保留过期）
    expired: bool = False          # 因保留规则被淘汰（区别于用户显式删除）
    hold: bool = False             # 显式冻结/保留，任何回收都不触碰


@dataclass
class UploadSession:
    upload_id: str
    key: str
    created_at: float
    expected_size: int | None = None
    expected_sha256: str | None = None
    parts: dict[int, PartInfo] = field(default_factory=dict)
    completed: bool = False
