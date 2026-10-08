"""后台回收与保留规则淘汰。

一轮维护做三类事：
1. **保留规则淘汰**：按 RetentionStore 中的 key/前缀规则，把超出
   “最多版本数/最长保留时间”的旧版本标记为 ``expired``；当前可见版本、
   显式冻结（hold）的版本绝不淘汰。
2. **数据回收**：已删除/已淘汰版本的数据文件、无元数据引用的孤儿文件，
   以及过期的上传会话被删除；正在被读取（active read）、被冻结的数据绝不删。
3. **容量同步**：删除的字节从 store 的已用容量中扣除。

并发安全：
- 元数据判定与标记全程在 ``store._lock`` 下完成（快，纯内存+小文件改写），
  保证不会把“写了一半尚未登记”的文件当孤儿删掉；
- 真正的数据文件删除在锁外执行：锁内只收集待删路径，避免大文件 IO 卡住
  正常读写/分片上传；读者在锁内登记 active read，只要文件尚未被删就会被
  收集阶段跳过，而一旦开始读取，GC 已经离开临界区也没关系——收集列表是
  在持锁瞬间确定的，新加入的 active read 不会影响已确定的列表，反之 GC
  收集时 active read 中的文件不会进入列表。
"""
from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

from .retention import RetentionStore
from .store import LocalObjectStore


class GarbageCollector:
    """扫描存储目录，回收无引用数据，并按保留规则淘汰旧版本。"""

    def __init__(
        self,
        store: LocalObjectStore,
        *,
        upload_ttl: float = 3600.0,
        retention: RetentionStore | None = None,
        clock=time.time,
    ):
        self.store = store
        self.upload_ttl = upload_ttl
        self.retention = retention or RetentionStore(store.root)
        self._clock = clock

    def collect_once(self) -> dict:
        """执行一轮维护，返回本轮统计。"""
        stats = {
            "expired_versions": 0,          # 本轮新被保留规则淘汰（标记）的版本数
            "reclaimed_version_files": 0,   # 删除的已删除/已淘汰版本数据文件数
            "reclaimed_orphan_files": 0,    # 删除的无引用孤儿文件数
            "reclaimed_bytes": 0,           # 上述文件合计字节
            "expired_uploads": 0,           # 清理的过期/残留上传会话数
            "reclaimed_upload_bytes": 0,    # 从分片清理中释放的字节
        }
        now = self._clock()

        version_files: list[tuple[Path, int]] = []
        orphan_files: list[tuple[Path, int]] = []
        upload_dirs: list[Path] = []
        planned_upload_freed = 0

        with self.store._lock:
            self._expire_and_collect_objects(now, stats, version_files, orphan_files)
            planned_upload_freed = self._collect_uploads(now, stats, upload_dirs)

        # ---- 锁外执行重 IO：删除文件/目录，不阻塞正常读写 ----
        planned = 0
        deleted = 0
        for path, size in version_files:
            planned += size
            if self._unlink(path):
                deleted += size
                stats["reclaimed_bytes"] += size
                stats["reclaimed_version_files"] += 1
        for path, size in orphan_files:
            planned += size
            if self._unlink(path):
                deleted += size
                stats["reclaimed_bytes"] += size
                stats["reclaimed_orphan_files"] += 1
        for path in upload_dirs:
            # 上传目录的容量释放已在持锁阶段按会话记账（planned_upload_freed），
            # 这里只做磁盘删除，不再参与版本文件的对账。
            size = self._dir_size(path)
            shutil.rmtree(path, ignore_errors=True)
            stats["reclaimed_upload_bytes"] += size
            stats["reclaimed_bytes"] += size
            stats["expired_uploads"] += 1

        # 版本/孤儿文件：持锁阶段已按计划量扣减；锁外若有删除失败则补回差额。
        if planned != deleted:
            with self.store._lock:
                self.store._used_bytes += planned - deleted
        # 上传会话：磁盘实际释放与记账（分片+占座）可能因崩溃残留而不一致，
        # 以实际目录大小为准校正。
        if stats["reclaimed_upload_bytes"] != planned_upload_freed:
            with self.store._lock:
                self.store._used_bytes += planned_upload_freed - stats["reclaimed_upload_bytes"]
        return stats

    # ------------------------------------------------------------------
    # 持锁阶段：判定 + 标记 + 收集路径
    # ------------------------------------------------------------------
    def _expire_and_collect_objects(
        self,
        now: float,
        stats: dict,
        version_files: list[tuple[Path, int]],
        orphan_files: list[tuple[Path, int]],
    ) -> None:
        store = self.store
        objects_dir = store.root / "objects"
        if not objects_dir.exists():
            return
        planned_version_bytes = 0
        for obj_dir in sorted(objects_dir.iterdir()):
            if not obj_dir.is_dir():
                continue
            meta_path = obj_dir / "meta.json"
            if not meta_path.exists():
                continue
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            key = meta["key"]
            versions: list[dict] = meta["versions"]

            # 1) 保留规则淘汰（只标记，不动数据）。
            rule = self.retention.rule_for(key)
            if rule is not None:
                self._apply_retention(rule, versions, now, stats)

            # 2) 收集待删的版本数据文件（删除/已淘汰，且未冻结、未在读）。
            referenced: set[str] = set()
            pending_delete: set[str] = set()
            changed = False
            versions_dir = obj_dir / "versions"
            for v in versions:
                is_dead = v.get("deleted") or v.get("delete_marker") or v.get("expired")
                if not is_dead:
                    referenced.add(v["version_id"])
                    continue
                # 删除标记没有数据文件；冻结版本即使被打了删除标记也不碰。
                if v.get("delete_marker") or v.get("hold"):
                    continue
                data_path = versions_dir / v["version_id"]
                if data_path.exists() and (key, v["version_id"]) not in store._active_reads:
                    size = data_path.stat().st_size
                    version_files.append((data_path, size))
                    planned_version_bytes += size
                    pending_delete.add(v["version_id"])
                if not v.get("data_reclaimed"):
                    v["data_reclaimed"] = True
                    changed = True
            if changed:
                store._save_meta(key, meta)

            # 3) 孤儿文件：无存活引用、且本轮没有作为“死版本数据”计划删除的文件。
            #    新写入遵循“先落文件、再在锁内登记引用”，因此走到这里时引用
            #    必然已可见，未被引用的文件只会是崩溃残留。
            if versions_dir.exists():
                for f in versions_dir.iterdir():
                    if f.name in referenced or f.name in pending_delete or f.name.startswith(".tmp-"):
                        continue
                    if f.is_file():
                        size = f.stat().st_size
                        orphan_files.append((f, size))
                        planned_version_bytes += size

        # 按计划删除量立即扣减容量；锁外若个别文件已不存在，会在 collect_once
        # 末尾对账补回，保证 used_bytes 与磁盘一致。
        store._used_bytes = max(0, store._used_bytes - planned_version_bytes)

    @staticmethod
    def _apply_retention(rule, versions: list[dict], now: float, stats: dict) -> None:
        """按规则给命中的旧版本打 expired 标记。

        候选：非删除标记、当前未删除/未淘汰、未冻结的数据版本（按时间从新到旧）。
        永远豁免：最新一个候选（即当前对外可见版本）与所有 hold 版本。
        """
        candidates = [
            v for v in versions
            if not v.get("delete_marker") and not v.get("deleted")
            and not v.get("expired") and not v.get("hold")
        ]
        candidates.sort(key=lambda v: v["created_at"], reverse=True)
        if not candidates:
            return
        # 当前对外可见版本：跳过删除标记/已删除后最新的那一个。
        visible = next(
            (
                v for v in sorted(versions, key=lambda x: x["created_at"], reverse=True)
                if not v.get("delete_marker") and not v.get("deleted") and not v.get("expired")
            ),
            None,
        )
        keep_by_count = rule.max_versions if rule.max_versions is not None else len(candidates)
        for idx, v in enumerate(candidates):
            if v is visible:
                continue
            old_enough = (
                rule.max_age_seconds is not None
                and now - v["created_at"] > rule.max_age_seconds
            )
            too_many = idx >= keep_by_count
            if old_enough or too_many:
                v["expired"] = True
                v["deleted"] = True  # 普通读取随即不可见，但可按版本号得到 version_expired
                stats["expired_versions"] += 1

    def _collect_uploads(self, now: float, stats: dict, upload_dirs: list[Path]) -> int:
        """返回按会话记账预计释放的字节数（已在本函数内扣减 used_bytes）。"""
        store = self.store
        uploads_dir = store.root / "uploads"
        if not uploads_dir.exists():
            return 0
        freed = 0
        for up_dir in sorted(uploads_dir.iterdir()):
            if not up_dir.is_dir():
                continue
            meta_path = up_dir / "meta.json"
            if not meta_path.exists():
                # 无元数据的残留目录：孤儿，锁外按实际大小清理（此处无法记账）。
                upload_dirs.append(up_dir)
                continue
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                upload_dirs.append(up_dir)
                continue
            expired = meta.get("completed") or now - meta["created_at"] > self.upload_ttl
            if expired:
                charged_parts = sum(p.get("size", 0) for p in meta.get("parts", {}).values())
                reserved = int(meta.get("reserved_bytes", 0))
                freed += charged_parts + max(0, reserved - charged_parts)
                upload_dirs.append(up_dir)
        store._used_bytes = max(0, store._used_bytes - freed)
        return freed

    # ------------------------------------------------------------------
    # 锁外辅助
    # ------------------------------------------------------------------
    @staticmethod
    def _unlink(path: Path) -> bool:
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _dir_size(path: Path) -> int:
        total = 0
        for dirpath, _dirs, files in os.walk(path):
            for name in files:
                try:
                    total += (Path(dirpath) / name).stat().st_size
                except FileNotFoundError:
                    pass
        return total
