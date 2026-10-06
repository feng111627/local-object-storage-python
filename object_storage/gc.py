"""旧版本与过期上传会话的回收。

回收只删除"无人引用"的 blob：扫描全部 key 的存活版本与所有进行中上传
会话的分片，构造引用集合后，再删除磁盘上的孤儿 blob。因此：

- 覆盖写产生的旧版本 blob 在旧版本被删除/过期之前仍被引用，不会被删；
- 被删除版本（墓碑，blob_id 已清空）与中止的分片上传，其 blob 在下次
  sweep 时成为孤儿而被回收；
- sweep 与并发提交之间由内容寻址的天然幂等保证安全：新提交的 blob
  在写元数据前已经落盘，而 sweep 先在本轮开始时收集引用快照——即使
  极端竞争，删除也是逐个再确认一次引用集合后进行，且读侧 blob 不可变。

可解释的回收时机：
1. 显式调用 ``POST /admin/gc``（或 ``service.garbage_collect()``）；
2. 后台线程按 ``gc_interval_seconds`` 周期执行（可关闭）；
3. sweep 同时中止超过 ``multipart_ttl_seconds`` 的未完成上传。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Iterable

from .config import StorageConfig
from .locks import LockRegistry
from .multipart import MultipartManager
from .storage import BlobStore

log = logging.getLogger("object_storage.gc")


class GarbageCollector:
    def __init__(
        self,
        blob_store: BlobStore,
        multipart: MultipartManager,
        locks: LockRegistry,
        config: StorageConfig,
        objects_dir: str,
    ) -> None:
        self.blobs = blob_store
        self.multipart = multipart
        self.locks = locks
        self.config = config
        self.objects_dir = objects_dir
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._sweep_lock = threading.Lock()

    def _iter_object_meta_files(self) -> Iterable[str]:
        if not os.path.isdir(self.objects_dir):
            return ()
        for name in os.listdir(self.objects_dir):
            if name.endswith(".json") and not name.startswith("."):
                yield os.path.join(self.objects_dir, name)

    def collect_referenced_blob_ids(self) -> set[str]:
        """收集当前所有存活版本 + 进行中分片引用的 blob id。"""
        referenced: set[str] = set()
        # 1) 对象版本
        for path in self._iter_object_meta_files():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                log.warning("skip unreadable object meta: %s", path)
                continue
            for version in data.get("versions", []):
                if version.get("deleteMarker"):
                    continue
                blob_id = version.get("blobId")
                if blob_id:
                    referenced.add(blob_id)
        # 2) 进行中的分片（含已完成但会话文件尚未终结的中间窗口）
        try:
            uploads = self.multipart.list_uploads()
        except OSError:
            uploads = []
        for upload in uploads:
            for part in upload.parts.values():
                referenced.add(part.blob_id)
            if upload.completed:
                try:
                    meta = self.multipart._read_completed_meta(upload.upload_id)
                    referenced.add(meta["blobId"])
                except OSError:
                    pass
        return referenced

    def _iter_blob_ids_on_disk(self) -> Iterable[str]:
        blobs_dir = self.blobs.blobs_dir
        if not os.path.isdir(blobs_dir):
            return ()
        for shard in os.listdir(blobs_dir):
            shard_path = os.path.join(blobs_dir, shard)
            if not os.path.isdir(shard_path) or len(shard) != 2:
                continue
            for name in os.listdir(shard_path):
                if len(name) == 64 and all(c in "0123456789abcdef" for c in name):
                    yield name

    def sweep(self) -> dict:
        """执行一次完整回收，返回统计信息。串行执行，避免并发 sweep。"""
        with self._sweep_lock:
            started = time.time()
            expired = self.abort_expired_uploads()
            referenced = self.collect_referenced_blob_ids()
            removed = 0
            bytes_freed = 0
            kept = 0
            for blob_id in list(self._iter_blob_ids_on_disk()):
                if blob_id in referenced:
                    kept += 1
                    continue
                path = self.blobs.blob_path(blob_id)
                try:
                    size = os.path.getsize(path)
                except OSError:
                    continue
                # 删除前再确认一次引用（防御极端的并发提交窗口）
                if blob_id in self.collect_referenced_blob_ids():
                    kept += 1
                    continue
                if self.blobs.delete_blob(blob_id):
                    removed += 1
                    bytes_freed += size
            # 清理残留 tmp 文件（崩溃中断的写入）
            tmp_removed = 0
            if os.path.isdir(self.blobs.tmp_dir):
                for name in os.listdir(self.blobs.tmp_dir):
                    try:
                        os.remove(os.path.join(self.blobs.tmp_dir, name))
                        tmp_removed += 1
                    except OSError:
                        pass
            stats = {
                "abortedUploads": expired["aborted"],
                "purgedCompletedSessions": expired["purgedCompleted"],
                "referencedBlobs": len(referenced),
                "removedBlobs": removed,
                "keptBlobs": kept,
                "removedTmpFiles": tmp_removed,
                "bytesFreed": bytes_freed,
                "elapsedSeconds": round(time.time() - started, 4),
            }
            log.info("gc sweep: %s", stats)
            return stats

    def abort_expired_uploads(self, now: float | None = None) -> dict:
        """清理过期会话：未完成的中止并释放槽位；已完成过幂等保护窗口
        的仅删除会话文件（其版本数据不受影响）。"""
        now = time.time() if now is None else now
        aborted = 0
        finalized_files = 0
        for upload in self.multipart.iter_expired_uploads(now):
            try:
                if upload.completed:
                    self.multipart.delete_session_files(upload.upload_id)
                    finalized_files += 1
                    log.info("purged completed session %s for key=%s",
                             upload.upload_id, upload.key)
                else:
                    self.multipart.abort_upload(upload.upload_id)
                    aborted += 1
                    log.info("aborted expired upload %s for key=%s age=%.1fs",
                             upload.upload_id, upload.key, now - upload.created_at)
            except Exception:
                log.exception("failed to expire upload %s", upload.upload_id)
        return {"aborted": aborted, "purgedCompleted": finalized_files}

    def start_background(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        if self.config.gc_interval_seconds <= 0:
            return
        self._stop.clear()

        def _run() -> None:
            log.info("background gc started, interval=%ss",
                     self.config.gc_interval_seconds)
            while not self._stop.wait(self.config.gc_interval_seconds):
                try:
                    self.sweep()
                except Exception:
                    log.exception("background gc sweep failed")

        self._thread = threading.Thread(
            target=_run, name="object-storage-gc", daemon=True
        )
        self._thread.start()

    def stop_background(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=5)
        self._thread = None
