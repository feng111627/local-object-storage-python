"""后台回收：清理被删除/淘汰且不再被读取引用的旧版本数据与过期会话。

回收时机与安全性：
- 只回收「元数据已标记删除/淘汰」或「没有任何元数据引用」的数据文件，
  冻结版本、以及元数据仍视为存活的版本绝不触碰；
- 逐 key 加锁回收，不持全局大锁，与其它 key 的读写完全并发；
- 删除数据前检查 read pin：版本正被读取时本轮跳过、下轮再试，绝不误删在用数据；
- 回收只删数据、保留元数据条目，因此「读取已删除/已淘汰版本」在回收前后
  都返回同样的 VersionAlreadyDeleted / VersionExpired，行为可解释。
"""
from __future__ import annotations

import json
import shutil
import time

from .store import LocalObjectStore


class GarbageCollector:
    """扫描存储目录，回收无引用的版本数据与过期上传会话。"""

    def __init__(self, store: LocalObjectStore, *, upload_ttl: float = 3600.0):
        self.store = store
        self.upload_ttl = upload_ttl

    def collect_once(self) -> dict:
        """执行一轮回收，返回回收统计。可由接口显式触发，也可由自动维护定时调用。"""
        stats = {
            "reclaimed_version_files": 0,
            "reclaimed_expired_files": 0,
            "reclaimed_bytes": 0,
            "reclaimed_orphan_files": 0,
            "skipped_pinned": 0,
            "expired_uploads": 0,
        }
        now = time.time()
        for key in self.store.iter_object_keys():
            self._collect_one_object(key, stats)
        self._collect_uploads(now, stats)
        return stats

    def _collect_one_object(self, key: str, stats: dict) -> None:
        # 持有该 key 的锁（不持全局大锁）：与其它 key 的读写完全并发。
        # 正常读取在拿到 pin 后就会释放 key 锁，因此这里不会长时间挡住读；
        # 同 key 的写本来就应与回收互斥，等待是正确语义。
        with self.store._key_locked(key):
            obj_dir = self.store._object_dir(key)
            meta_path = obj_dir / "meta.json"
            if not meta_path.exists():
                return
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            referenced = set()
            changed = False
            for v in meta["versions"]:
                referenced.add(v["version_id"])
                removable = v.get("deleted") or v.get("delete_marker") or v.get("expired")
                if not removable or v.get("frozen"):
                    continue
                data_path = obj_dir / "versions" / v["version_id"]
                if not data_path.exists():
                    if not v.get("data_reclaimed"):
                        v["data_reclaimed"] = True
                        changed = True
                    continue
                reclaimed = self.store.reclaim_data_file(key, v["version_id"], data_path)
                if reclaimed == 0:
                    # 仍被读取：本轮跳过，标记与文件都保留，下轮再清。
                    stats["skipped_pinned"] += 1
                    continue
                stats["reclaimed_bytes"] += reclaimed
                stats["reclaimed_version_files"] += 1
                if v.get("expired"):
                    stats["reclaimed_expired_files"] += 1
                v["data_reclaimed"] = True
                changed = True
            if changed:
                self.store._save_meta(meta["key"], meta)
            # 孤儿文件：磁盘上存在但没有任何元数据引用（如崩溃中断的写入残留）。
            versions_dir = obj_dir / "versions"
            if versions_dir.exists():
                for f in list(versions_dir.iterdir()):
                    if not f.is_file():
                        continue
                    if f.name.startswith(".tmp-") or f.name not in referenced:
                        size = f.stat().st_size
                        f.unlink()
                        stats["reclaimed_bytes"] += size
                        stats["reclaimed_orphan_files"] += 1
                        self.store._release_usage(size)

    def _collect_uploads(self, now: float, stats: dict) -> None:
        uploads_dir = self.store.root / "uploads"
        if not uploads_dir.exists():
            return
        for up_dir in list(uploads_dir.iterdir()):
            if not up_dir.is_dir():
                continue
            meta_path = up_dir / "meta.json"
            if not meta_path.exists():
                shutil.rmtree(up_dir, ignore_errors=True)
                stats["expired_uploads"] += 1
                continue
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("completed") or now - meta["created_at"] > self.upload_ttl:
                removed = sum(p["size"] for p in meta.get("parts", {}).values())
                shutil.rmtree(up_dir, ignore_errors=True)
                stats["expired_uploads"] += 1
                if removed:
                    self.store._release_usage(removed)
