"""后台回收：清理被覆盖/删除且不再被引用的旧版本数据。

回收时机与安全性：
- 只回收「元数据已标记删除」或「没有任何元数据引用」的数据文件，
  仍被版本清单引用的文件绝不触碰；
- 整个回收过程持有 store 的锁，与读写/上传操作互斥，
  因此不会回收正在写入但尚未登记元数据的文件；
- 回收只删数据、保留元数据条目，因此「读取已删除版本」在回收前后
  都返回同样的 VersionAlreadyDeleted，行为可解释。
"""
from __future__ import annotations

import json
import shutil
import time
from urllib.parse import unquote

from .store import LocalObjectStore


class GarbageCollector:
    """扫描存储目录，回收无引用的版本数据与过期上传会话。"""

    def __init__(self, store: LocalObjectStore, *, upload_ttl: float = 3600.0):
        self.store = store
        self.upload_ttl = upload_ttl

    def collect_once(self) -> dict:
        """执行一轮回收，返回回收统计（可解释的时机：由调用方显式触发）。"""
        stats = {
            "reclaimed_version_files": 0,
            "reclaimed_bytes": 0,
            "reclaimed_orphan_files": 0,
            "expired_uploads": 0,
        }
        now = time.time()
        with self.store._lock:
            self._collect_objects(stats)
            self._collect_uploads(now, stats)
        return stats

    def _collect_objects(self, stats: dict) -> None:
        objects_dir = self.store.root / "objects"
        for obj_dir in objects_dir.iterdir():
            if not obj_dir.is_dir():
                continue
            meta_path = obj_dir / "meta.json"
            if not meta_path.exists():
                continue
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            referenced = set()
            changed = False
            for v in meta["versions"]:
                if v.get("deleted") or v.get("delete_marker"):
                    # 已删除版本：回收数据文件，保留元数据条目。
                    data_path = obj_dir / "versions" / v["version_id"]
                    if data_path.exists():
                        stats["reclaimed_bytes"] += data_path.stat().st_size
                        data_path.unlink()
                        stats["reclaimed_version_files"] += 1
                    if not v.get("data_reclaimed"):
                        v["data_reclaimed"] = True
                        changed = True
                else:
                    referenced.add(v["version_id"])
            if changed:
                self.store._save_meta(meta["key"], meta)
            # 孤儿文件：磁盘上存在但没有任何元数据引用（如崩溃中断的写入残留）。
            versions_dir = obj_dir / "versions"
            if versions_dir.exists():
                for f in versions_dir.iterdir():
                    if f.name.startswith(".tmp-") or f.name not in referenced:
                        stats["reclaimed_bytes"] += f.stat().st_size
                        f.unlink()
                        stats["reclaimed_orphan_files"] += 1

    def _collect_uploads(self, now: float, stats: dict) -> None:
        uploads_dir = self.store.root / "uploads"
        for up_dir in uploads_dir.iterdir():
            if not up_dir.is_dir():
                continue
            meta_path = up_dir / "meta.json"
            if not meta_path.exists():
                shutil.rmtree(up_dir, ignore_errors=True)
                stats["expired_uploads"] += 1
                continue
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("completed") or now - meta["created_at"] > self.upload_ttl:
                shutil.rmtree(up_dir, ignore_errors=True)
                stats["expired_uploads"] += 1
