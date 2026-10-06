"""后台回收测试：删除版本数据回收、孤儿文件、过期会话、引用保护。"""
from __future__ import annotations

import logging
import time

import pytest

from object_storage.errors import NoSuchUpload, VersionAlreadyDeleted
from object_storage.gc import GarbageCollector

logger = logging.getLogger("object_storage.tests.gc")


class TestGarbageCollection:
    def test_deleted_version_data_reclaimed(self, store, gc):
        """被删除版本的数据文件被回收，元数据保留（错误语义不变）。"""
        logger.info("业务覆盖：已删除版本的数据回收")
        v = store.put_object("k", b"x" * 100)
        store.delete_object("k", v.version_id)
        stats = gc.collect_once()
        assert stats["reclaimed_version_files"] == 1
        assert stats["reclaimed_bytes"] == 100
        # 元数据仍在：读已删除版本仍返回可区分的 VersionAlreadyDeleted
        with pytest.raises(VersionAlreadyDeleted):
            store.get_object("k", v.version_id)

    def test_live_versions_never_reclaimed(self, store, gc):
        """回收绝不影响仍被引用的版本。"""
        logger.info("业务覆盖：回收对存活版本的引用保护")
        v1 = store.put_object("k", b"old")
        v2 = store.put_object("k", b"new")
        stats = gc.collect_once()
        assert stats["reclaimed_version_files"] == 0
        assert store.get_object("k", v1.version_id)[1] == b"old"
        assert store.get_object("k", v2.version_id)[1] == b"new"

    def test_overwritten_versions_reclaimed_after_delete(self, store, gc):
        """被覆盖的旧版本在删除后被回收，新版本不受影响。"""
        logger.info("业务覆盖：覆盖写旧版本的回收时机")
        v1 = store.put_object("k", b"v1")
        store.put_object("k", b"v2")
        store.delete_object("k", v1.version_id)
        stats = gc.collect_once()
        assert stats["reclaimed_version_files"] == 1
        assert store.get_object("k")[1] == b"v2"

    def test_orphan_files_reclaimed(self, store, gc, tmp_path):
        """崩溃残留的孤儿文件（无元数据引用）被回收。"""
        logger.info("业务覆盖：异常中断残留的孤儿文件回收")
        store.put_object("k", b"live")
        versions_dir = store.root / "objects" / "k" / "versions"
        orphan = versions_dir / "orphan-crashed-write"
        orphan.write_bytes(b"garbage")
        stats = gc.collect_once()
        assert stats["reclaimed_orphan_files"] >= 1
        assert not orphan.exists()
        assert store.get_object("k")[1] == b"live"

    def test_expired_uploads_aborted(self, store):
        """超过 TTL 的未完成会话被回收，名额随之释放。"""
        logger.info("业务覆盖：过期上传会话回收")
        gc = GarbageCollector(store, upload_ttl=0.0)
        up = store.create_multipart_upload("k")
        store.upload_part(up.upload_id, 1, b"x")
        stats = gc.collect_once()
        assert stats["expired_uploads"] == 1
        with pytest.raises(NoSuchUpload):
            store.list_parts(up.upload_id)

    def test_fresh_uploads_not_expired(self, store):
        """未过期的活跃会话不会被回收。"""
        logger.info("业务覆盖：活跃会话保护")
        gc = GarbageCollector(store, upload_ttl=3600.0)
        up = store.create_multipart_upload("k")
        stats = gc.collect_once()
        assert stats["expired_uploads"] == 0
        store.upload_part(up.upload_id, 1, b"x")  # 会话仍可用

    def test_gc_is_idempotent(self, store, gc):
        """重复执行回收：第二轮无可回收内容，统计归零。"""
        logger.info("业务覆盖：回收幂等性")
        v = store.put_object("k", b"data")
        store.delete_object("k", v.version_id)
        first = gc.collect_once()
        second = gc.collect_once()
        assert first["reclaimed_version_files"] == 1
        assert second["reclaimed_version_files"] == 0
        assert second["reclaimed_bytes"] == 0
