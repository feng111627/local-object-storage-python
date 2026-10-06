"""服务配置：配额与回收策略。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StorageConfig:
    # 单个对象整体大小上限（字节）
    max_object_size: int = 5 * 1024 * 1024 * 1024
    # 单次上传允许的分片数量上限
    max_parts_per_upload: int = 10_000
    # 同一服务实例内进行中的分片上传会话数上限
    max_concurrent_uploads: int = 100
    # 初始化时声明的单分片大小（除最后一个分片外都必须达到该大小）
    min_part_size: int = 5 * 1024 * 1024
    # 未完成上传会话的保留时长，超时可被回收（秒）
    multipart_ttl_seconds: float = 24 * 3600.0
    # 后台回收轮询间隔（秒）；<=0 表示不启动后台线程
    gc_interval_seconds: float = 60.0
    # 是否允许后台回收线程
    enable_background_gc: bool = True
