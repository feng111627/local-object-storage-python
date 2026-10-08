"""存储总容量上限测试。

覆盖：整体写/分片/合并三类写入的超限拒绝、错误码与单对象配额可区分、
不会只写入一部分当成功、回收腾出空间后被拒写入可重试成功、
用量账本准确、重启后以磁盘实际占用重建账本。
"""
from __future__ import annotations

import logging

import pytest

from object_storage.errors import (
    ObjectTooLarge,
    StorageCapacityExceeded,
)
from object_storage.gc import GarbageCollector
from object_storage.models import QuotaConfig
from object_storage.store import LocalObjectStore

logger = logging.getLogger("object_storage.tests.capacity")


@pytest.fixture()
def capped_store(tmp_path):
    return LocalObjectStore(tmp_path / "data", QuotaConfig(max_total_size=100))


class TestTotalCapacity:
    def test_put_rejected_over_cap_with_distinct_code(self, capped_store):
        logger.info("业务覆盖：整体写超总容量被拒，错误码可区分")
        capped_store.put_object("a", b"x" * 60)
        with pytest.raises(StorageCapacityExceeded) as ei:
            capped_store.put_object("b", b"y" * 50)
        assert ei.value.code == "storage_capacity_exceeded"
        # 被拒写入没有留下任何数据
        assert capped_store.usage()["used_bytes"] == 60

    def test_capacity_distinct_from_object_size_quota(self, tmp_path):
        logger.info("业务覆盖：单对象配额与总容量是两个不同错误")
        s = LocalObjectStore(tmp_path / "d", QuotaConfig(max_object_size=10, max_total_size=1000))
        with pytest.raises(ObjectTooLarge):
            s.put_object("a", b"x" * 11)

    def test_no_partial_success(self, capped_store):
        logger.info("业务覆盖：超容量写入不会先落一部分数据")
        capped_store.put_object("a", b"x" * 90)
        before = list((capped_store.root / "objects").rglob("*"))
        with pytest.raises(StorageCapacityExceeded):
            capped_store.put_object("a", b"z" * 20)
        # 没有产生新的版本文件
        after = list((capped_store.root / "objects").rglob("*"))
        assert len(after) == len(before)

    def test_part_upload_rejected_over_cap(self, capped_store):
        logger.info("业务覆盖：新分片超总容量被拒，会话与已有分片不受影响")
        up = capped_store.create_multipart_upload("big")
        capped_store.upload_part(up.upload_id, 1, b"x" * 60)
        with pytest.raises(StorageCapacityExceeded):
            capped_store.upload_part(up.upload_id, 2, b"y" * 50)
        parts = capped_store.list_parts(up.upload_id)
        assert [p.part_number for p in parts] == [1]
        assert capped_store.usage()["used_bytes"] == 60

    def test_complete_rejected_when_projected_exceeds(self, tmp_path):
        logger.info("业务覆盖：合并结果使总占用超限时拒绝，分片保留可重试")
        s = LocalObjectStore(tmp_path / "d", QuotaConfig(max_total_size=100, max_object_size=1000))
        # occupy 60 + 分片 30 = 90，合并时刻「最终占用」90 尚允许
        occupy = s.put_object("occupy", b"o" * 60)
        up = s.create_multipart_upload("big")
        s.upload_part(up.upload_id, 1, b"a" * 30)
        # 直接把容量调小：模拟「创建会话后运维收紧总容量」，
        # 于是分片落盘时允许、合并判定时拒绝。
        s.quota = QuotaConfig(max_total_size=80, max_object_size=1000)
        with pytest.raises(StorageCapacityExceeded):
            s.complete_multipart_upload(up.upload_id)
        # 分片仍在；回收掉 occupy 后腾出空间，可以合并成功
        s.delete_object("occupy", occupy.version_id)
        GarbageCollector(s).collect_once()
        v = s.complete_multipart_upload(up.upload_id)
        assert s.get_object("big")[1] == b"a" * 30
        assert v.size == 30

    def test_rejected_write_succeeds_after_space_reclaimed(self, capped_store):
        logger.info("业务覆盖：回收腾出空间后，之前被拒的写入重新成功")
        v = capped_store.put_object("k", b"x" * 80)
        with pytest.raises(StorageCapacityExceeded):
            capped_store.put_object("k2", b"y" * 50)
        capped_store.delete_object("k", v.version_id)
        GarbageCollector(capped_store).collect_once()
        # 80 字节腾出来了
        v2 = capped_store.put_object("k2", b"y" * 50)
        assert v2.size == 50
        assert capped_store.usage()["used_bytes"] == 50

    def test_abort_frees_capacity(self, capped_store):
        logger.info("业务覆盖：中止分片上传释放占用")
        up = capped_store.create_multipart_upload("big")
        capped_store.upload_part(up.upload_id, 1, b"x" * 60)
        capped_store.abort_multipart_upload(up.upload_id)
        assert capped_store.usage()["used_bytes"] == 0
        # 容量回来后新写入成功
        capped_store.put_object("other", b"z" * 90)

    def test_usage_accounting_tracks_multipart_lifecycle(self, tmp_path):
        logger.info("业务覆盖：分片上传全流程的用量账实相符")
        s = LocalObjectStore(tmp_path / "d", QuotaConfig(max_total_size=10_000))
        up = s.create_multipart_upload("m")
        assert s.usage()["used_bytes"] == 0
        s.upload_part(up.upload_id, 1, b"a" * 10)
        s.upload_part(up.upload_id, 2, b"bb" * 10)
        assert s.usage()["used_bytes"] == 30
        s.complete_multipart_upload(up.upload_id)
        # 合并后只剩对象本身 30 字节
        assert s.usage()["used_bytes"] == 30

    def test_usage_reconciled_on_restart(self, tmp_path):
        logger.info("业务覆盖：重启后按磁盘实际占用重建账本")
        s1 = LocalObjectStore(tmp_path / "d", QuotaConfig(max_total_size=1000))
        s1.put_object("a", b"x" * 123)
        up = s1.create_multipart_upload("m")
        s1.upload_part(up.upload_id, 1, b"p" * 7)
        s2 = LocalObjectStore(tmp_path / "d", QuotaConfig(max_total_size=1000))
        assert s2.usage()["used_bytes"] == 130
        # 账本准确后，总容量判定继续生效
        with pytest.raises(StorageCapacityExceeded):
            s2.put_object("b", b"q" * 900)
