"""总容量上限测试：各写入环节的拒绝规则、容量回收后恢复、重启重建。"""
from __future__ import annotations

import logging

import pytest

from object_storage import GarbageCollector, LocalObjectStore, RetentionStore
from object_storage.errors import TotalCapacityExceeded

logger = logging.getLogger("object_storage.tests.capacity")


def make_store(tmp_path, limit=100):
    return LocalObjectStore(tmp_path / "data", quota=None) if False else LocalObjectStore(
        tmp_path / "data",
        __import__("object_storage", fromlist=["QuotaConfig"]).QuotaConfig(
            max_object_size=10_000, max_total_bytes=limit
        ),
    )


class TestPutCapacity:
    def test_put_rejected_over_total_capacity(self, tmp_path):
        """整体写入超总容量 -> total_capacity_exceeded，detail 带判定依据。"""
        logger.info("业务覆盖：PUT 总容量超限拒绝且不产生半成品")
        store = make_store(tmp_path, limit=10)
        info = store.put_object("k", b"x" * 10)
        assert store.used_bytes == 10
        with pytest.raises(TotalCapacityExceeded) as ei:
            store.put_object("k2", b"y")
        assert ei.value.detail["scope"] == "put_object"
        assert ei.value.detail["used"] == 10 and ei.value.detail["limit"] == 10
        # 被拒写入没有留下任何数据
        assert store.used_bytes == 10
        versions_dir = tmp_path / "data" / "objects"
        leftovers = [p for p in versions_dir.rglob("*") if p.is_file() and p.name != "meta.json"]
        assert len(leftovers) == 1

    def test_capacity_freed_after_gc_then_put_succeeds(self, tmp_path):
        """回收腾空间后，之前因容量被拒的写入重新成功。"""
        logger.info("业务覆盖：回收释放容量后写入恢复")
        store = make_store(tmp_path, limit=10)
        retention = RetentionStore(tmp_path / "data")
        gc = GarbageCollector(store, retention=retention, upload_ttl=9999)
        store.put_object("k", b"x" * 10)
        with pytest.raises(TotalCapacityExceeded):
            store.put_object("k2", b"y" * 5)
        # 删除第一个版本并回收 -> 腾出 10 字节
        vid = store.list_versions("k")[0].version_id
        store.delete_object("k", vid)
        stats = gc.collect_once()
        assert stats["reclaimed_bytes"] == 10
        assert store.used_bytes == 0
        store.put_object("k2", b"y" * 5)
        assert store.used_bytes == 5

    def test_new_versions_of_same_key_count_toward_total(self, tmp_path):
        """同 key 多个版本的历史数据同样占总容量。"""
        logger.info("业务覆盖：历史版本占用总容量")
        store = make_store(tmp_path, limit=15)
        store.put_object("k", b"a" * 8)
        store.put_object("k", b"b" * 7)
        assert store.used_bytes == 15
        with pytest.raises(TotalCapacityExceeded):
            store.put_object("k", b"c")


class TestMultipartCapacity:
    def test_new_upload_reservation(self, tmp_path):
        """声明 expected_size 的新会话按大小占座，第二个会话无空间即拒。"""
        logger.info("业务覆盖：新分片会话按声明大小占座")
        store = make_store(tmp_path, limit=10)
        s1 = store.create_multipart_upload("a", expected_size=8)
        assert store.used_bytes == 8
        with pytest.raises(TotalCapacityExceeded) as ei:
            store.create_multipart_upload("b", expected_size=3)
        assert ei.value.detail["scope"] == "new_upload"
        # 中止会话释放占座
        store.abort_multipart_upload(s1.upload_id)
        assert store.used_bytes == 0
        store.create_multipart_upload("b", expected_size=3)

    def test_new_upload_reservation(self, tmp_path):
        """声明 expected_size 的新会话按 2 倍大小占座（覆盖合并瞬时峰值），
        放不下即拒；中止后占座全额释放。"""
        logger.info("业务覆盖：新分片会话按峰值占座")
        store = make_store(tmp_path, limit=20)
        s1 = store.create_multipart_upload("a", expected_size=8)  # 占 16
        assert store.used_bytes == 16
        with pytest.raises(TotalCapacityExceeded) as ei:
            store.create_multipart_upload("b", expected_size=3)  # 再需 6，只剩 4
        assert ei.value.detail["scope"] == "new_upload"
        # 中止会话释放占座
        store.abort_multipart_upload(s1.upload_id)
        assert store.used_bytes == 0
        store.create_multipart_upload("b", expected_size=3)

    def test_part_upload_uses_reservation(self, tmp_path):
        """分片先抵扣占座；分片累计不超过最终大小时不产生新增占用，
        超过占座（即累计分片超过声明大小）且总容量已满时拒绝。"""
        logger.info("业务覆盖：分片上传抵扣占座与超额拒绝")
        store = make_store(tmp_path, limit=10)
        s = store.create_multipart_upload("a", expected_size=5)  # 占 10
        store.upload_part(s.upload_id, 1, b"12345")  # 全部落在占座内
        assert store.used_bytes == 10
        with pytest.raises(TotalCapacityExceeded) as ei:
            store.upload_part(s.upload_id, 2, b"x" * 6)  # 超出占座 1 字节且容量已满
        assert ei.value.detail["scope"] == "upload_part"
        # 不超过占座余量的分片照常接收
        store.upload_part(s.upload_id, 2, b"x" * 3)
        assert store.used_bytes == 10

    def test_unreserved_upload_parts_rejected(self, tmp_path):
        """未声明大小的会话：分片按实际占用计费，超额拒绝。"""
        logger.info("业务覆盖：无声明分片按实际占用拒绝")
        store = make_store(tmp_path, limit=10)
        s = store.create_multipart_upload("a")
        store.upload_part(s.upload_id, 1, b"x" * 6)
        assert store.used_bytes == 6
        with pytest.raises(TotalCapacityExceeded):
            store.upload_part(s.upload_id, 2, b"y" * 5)  # 6+5 > 10

    def test_complete_rejected_when_result_does_not_fit(self, tmp_path):
        """无占座时合并产物与分片同时占盘：放不下则拒绝且不产生版本。"""
        logger.info("业务覆盖：合并结果总容量不足拒绝，不留半成品版本")
        store = make_store(tmp_path, limit=9)
        s = store.create_multipart_upload("a")
        store.upload_part(s.upload_id, 1, b"1234")  # 4
        store.upload_part(s.upload_id, 2, b"5678")  # 8
        # 合并需要额外 8 字节产物空间，8+8 > 9
        with pytest.raises(TotalCapacityExceeded) as ei:
            store.complete_multipart_upload(s.upload_id)
        assert ei.value.detail["scope"] == "complete_upload"
        # 分片仍在，会话未完成，可在空间腾出后重试
        assert len(store.list_parts(s.upload_id)) == 2
        # 模拟腾空间（中止后整体容量回归）——这里直接验证可重试路径：
        # 放大限额做不到，改为验证占座场景下合并能成功（下一个用例）。

    def test_complete_succeeds_within_reservation(self, tmp_path):
        """占座覆盖最终大小与合并峰值时，合并顺畅完成且容量最终只算一份。"""
        logger.info("业务覆盖：占座覆盖下合并成功，容量结算正确")
        store = make_store(tmp_path, limit=20)
        s = store.create_multipart_upload("a", expected_size=8)  # 占 16
        store.upload_part(s.upload_id, 1, b"1234")
        store.upload_part(s.upload_id, 2, b"5678")
        v = store.complete_multipart_upload(s.upload_id)
        assert v.size == 8
        assert store.used_bytes == 8  # 分片已删，只剩版本数据
        assert store.get_object("a")[1] == b"12345678"

    def test_abort_releases_parts_and_reservation(self, tmp_path):
        logger.info("业务覆盖：中止会话释放实际分片占用与占座")
        store = make_store(tmp_path, limit=40)
        s = store.create_multipart_upload("a", expected_size=15)  # 占 30
        store.upload_part(s.upload_id, 1, b"x" * 5)  # charged 5，unused reserve 25
        store.abort_multipart_upload(s.upload_id)
        assert store.used_bytes == 0


class TestCapacityPersistence:
    def test_used_bytes_rebuilt_on_restart(self, tmp_path):
        """重启后扫描磁盘重建已用容量，包含版本与残留分片。"""
        logger.info("业务覆盖：容量基线重启重建")
        from object_storage.models import QuotaConfig
        path = tmp_path / "data"
        s1 = LocalObjectStore(path, QuotaConfig(max_object_size=10_000, max_total_bytes=1000))
        s1.put_object("k", b"a" * 11)
        up = s1.create_multipart_upload("m", expected_size=20)
        s1.upload_part(up.upload_id, 1, b"b" * 7)
        s2 = LocalObjectStore(path, QuotaConfig(max_object_size=10_000, max_total_bytes=1000))
        # 磁盘上：11 字节版本 + 7 字节分片（占座是内存记账，重启后不再计入）
        assert s2.used_bytes == 18
        cap = s2.capacity()
        assert cap["limit_bytes"] == 1000 and cap["available_bytes"] == 982
