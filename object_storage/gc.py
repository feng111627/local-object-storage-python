"""后台回收：清理被覆盖/删除且不再被引用的旧版本数据。"""
from __future__ import annotations

from .store import LocalObjectStore


class GarbageCollector:
    """扫描存储，回收无引用的版本数据与过期上传会话。"""

    def __init__(self, store: LocalObjectStore, *, upload_ttl: float = 3600.0):
        self.store = store
        self.upload_ttl = upload_ttl

    def collect_once(self) -> dict:
        """执行一轮回收，返回回收统计。"""
        raise NotImplementedError
