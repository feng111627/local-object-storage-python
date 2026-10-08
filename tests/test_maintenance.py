"""自动维护调度器与并发安全测试。"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from object_storage import GarbageCollector, LocalObjectStore, MaintenanceScheduler, RetentionStore
from object_storage.errors import VersionExpired

logger = logging.getLogger("object_storage.tests.maintenance")


@pytest.fixture()
def retention(tmp_path):
    return RetentionStore(tmp_path / "data")


def make_scheduler(store, retention, interval=0.05):
    gc = GarbageCollector(store, retention=retention, upload_ttl=9999.0)
    return MaintenanceScheduler(gc, interval_seconds=interval)


class TestScheduler:
    def test_manual_run_records_status(self, tmp_path, retention):
        """手动触发一轮：状态里能看到最近时间、统计、累计轮次。"""
        logger.info("业务覆盖：手动触发维护并查询可观测状态")
        store = LocalObjectStore(tmp_path / "data")
        sched = make_scheduler(store, retention)
        retention.set_rule("key", "k", max_versions=1)
        store.put_object("k", b"a")
        time.sleep(0.002)
        store.put_object("k", b"b")
        stats = sched.run_once(force=True)
        assert stats and stats["expired_versions"] == 1
        st = sched.status()
        assert st["total_runs"] == 1
        assert st["last_run_at"] is not None
        assert st["last_stats"]["expired_versions"] == 1
        assert st["currently_running"] is False
        assert st["scheduler_running"] is False
        assert "capacity" in st

    def test_scheduler_runs_automatically(self, tmp_path, retention):
        """启动调度器后到点自动跑，无需人工触发。"""
        logger.info("业务覆盖：定时自动触发维护")
        store = LocalObjectStore(tmp_path / "data")
        sched = make_scheduler(store, retention, interval=0.03)
        retention.set_rule("key", "k", max_versions=1)
        store.put_object("k", "a".encode())
        time.sleep(0.002)
        store.put_object("k", "b".encode())
        sched.start()
        try:
            deadline = time.time() + 3
            while time.time() < deadline and sched.status()["total_runs"] == 0:
                time.sleep(0.01)
            assert sched.status()["total_runs"] >= 1
        finally:
            sched.stop()
        assert sched.status()["scheduler_running"] is False

    def test_overlapping_runs_are_skipped(self, tmp_path, retention):
        """上一轮没跑完时，新的触发被跳过而不是排队重入。"""
        logger.info("业务覆盖：维护轮次互斥，重叠触发跳过")
        store = LocalObjectStore(tmp_path / "data")
        entered = threading.Event()
        release = threading.Event()

        class SlowGC(GarbageCollector):
            def collect_once(self):
                entered.set()
                release.wait(timeout=5)
                return super().collect_once()

        gc = SlowGC(store, retention=retention, upload_ttl=9999.0)
        sched = MaintenanceScheduler(gc, interval_seconds=10)
        t = threading.Thread(target=sched.run_once, kwargs={"force": True})
        t.start()
        entered.wait(timeout=2)
        assert sched.run_once() is None  # 非强制触发直接跳过
        release.set()
        t.join()

    def test_total_runs_persist_across_restart(self, tmp_path, retention):
        """累计轮次持久化，新建调度器后接着计数。"""
        logger.info("业务覆盖：维护轮次计数重启不丢")
        path = tmp_path / "data"
        store = LocalObjectStore(path)
        s1 = make_scheduler(store, retention)
        s1.run_once(force=True)
        s1.run_once(force=True)
        store2 = LocalObjectStore(path)
        s2 = make_scheduler(store2, retention)
        assert s2.status()["total_runs"] == 2
        s2.run_once(force=True)
        assert s2.status()["total_runs"] == 3

    def test_failing_round_does_not_kill_scheduler(self, tmp_path, retention):
        """单轮异常被记录且不会让后台线程死亡，后续轮次仍能跑。"""
        logger.info("业务覆盖：单轮维护失败可观测且调度器存活")
        store = LocalObjectStore(tmp_path / "data")

        class FlakyGC(GarbageCollector):
            def __init__(self, *a, **kw):
                super().__init__(*a, **kw)
                self.calls = 0

            def collect_once(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("boom")
                return {"expired_versions": 0, "reclaimed_bytes": 0}

        gc = FlakyGC(store, retention=retention)
        sched = MaintenanceScheduler(gc, interval_seconds=10)
        with pytest.raises(RuntimeError):
            sched.run_once(force=True)
        assert "boom" in sched.status()["last_error"]
        sched.run_once(force=True)
        assert sched.status()["last_error"] is None


class TestConcurrentAccess:
    def test_gc_concurrent_with_reads_never_loses_live_data(self, tmp_path, retention):
        """回收与大量并发读同时进行：存活版本始终可读、内容完整。"""
        logger.info("业务覆盖：回收并发读取，存活数据不被误删、不读半成品")
        store = LocalObjectStore(tmp_path / "data")
        gc = GarbageCollector(store, retention=retention, upload_ttl=0.001)
        retention.set_rule("prefix", "k", max_versions=1)
        versions = []
        for i in range(6):
            versions.append(store.put_object("k", f"v{i}".encode()).version_id)
            time.sleep(0.002)

        errors: list[Exception] = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                try:
                    # 最新版本必须始终完整可读（内容只能是写入过的 v* 或 n*）。
                    _info, data = store.get_object("k")
                    assert data[:1] in (b"v", b"n")
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for _ in range(10):
            gc.collect_once()
            time.sleep(0.001)
            # 持续制造新版本，给淘汰制造压力
            versions.append(store.put_object("k", f"n{time.time()}".encode()).version_id)
        stop.set()
        for t in threads:
            t.join()
        assert errors == []
        assert store.get_object("k")[0].version_id == versions[-1]

    def test_gc_blocked_until_inflight_read_finishes(self, tmp_path, retention):
        """存活版本正在读取的窗口内，GC 被存储锁挡在门外；
        读者一定拿到完整内容，读完之后删除+回收才能进行。"""
        logger.info("业务覆盖：活跃读与回收互斥，读期间不误删、不读半成品")
        store = LocalObjectStore(tmp_path / "data")
        gc = GarbageCollector(store, retention=retention, upload_ttl=9999.0)
        v = store.put_object("k", b"payload")

        read_entered = threading.Event()
        allow_read_finish = threading.Event()
        gc_done = threading.Event()
        outcome = {}

        import pathlib
        orig_read_bytes = pathlib.Path.read_bytes

        def patched_read_bytes(self):
            if self.name == v.version_id:
                read_entered.set()
                allow_read_finish.wait(timeout=5)
            return orig_read_bytes(self)

        pathlib.Path.read_bytes = patched_read_bytes
        try:
            t = threading.Thread(target=lambda: outcome.setdefault("data", store.get_object("k")[1]))
            t.start()
            read_entered.wait(timeout=2)
            # 读取持锁期间：GC 只能排队等待，无法完成。
            def run_gc():
                outcome.setdefault("gc_stats", gc.collect_once())
                gc_done.set()

            gct = threading.Thread(target=run_gc)
            gct.start()
            time.sleep(0.05)
            assert not gc_done.is_set(), "GC 不应在活跃读未结束时完成"
            assert (store._object_dir("k") / "versions" / v.version_id).exists()
            allow_read_finish.set()
            t.join()
            gct.join(timeout=5)
        finally:
            pathlib.Path.read_bytes = orig_read_bytes

        assert outcome["data"] == b"payload"
        assert outcome["gc_stats"]["reclaimed_version_files"] == 0  # 版本当时仍存活
        # 读完之后删除，再回收：文件被正常清掉
        store.delete_object("k", v.version_id)
        assert gc.collect_once()["reclaimed_version_files"] == 1

    def test_concurrent_puts_and_maintenance_accounting_consistent(self, tmp_path, retention):
        """并发整体写入 + 多轮维护下：容量记账不出现负数，成功写入的都可读。"""
        logger.info("业务覆盖：并发写入与维护下容量记账一致")
        from object_storage.models import QuotaConfig
        store = LocalObjectStore(tmp_path / "data", QuotaConfig(max_total_bytes=10_000_000))
        gc = GarbageCollector(store, retention=retention, upload_ttl=9999.0)
        retention.set_rule("prefix", "c/", max_versions=3)
        written: list[tuple[str, str, bytes]] = []
        lock = threading.Lock()

        def writer(seed):
            for i in range(20):
                key = f"c/{seed % 4}"
                payload = f"{seed}-{i}".encode()
                info = store.put_object(key, payload)
                with lock:
                    written.append((key, info.version_id, payload))

        threads = [threading.Thread(target=writer, args=(s,)) for s in range(6)]
        for t in threads:
            t.start()
        collector = threading.Thread(
            target=lambda: [gc.collect_once() or time.sleep(0.002) for _ in range(20)]
        )
        collector.start()
        for t in threads:
            t.join()
        collector.join()
        assert store.used_bytes >= 0
        # 全部成功写入的版本按版本号都能读回（未被误删的存活版本）
        missing = 0
        alive = 0
        for key, vid, payload in written:
            try:
                _info, data = store.get_object(key, vid)
                assert data == payload
                alive += 1
            except VersionExpired:
                missing += 1  # 被保留规则淘汰是允许的，但必须是明确错误而非损坏
        assert alive + missing == len(written)
        gc.collect_once()
        assert store.used_bytes >= 0
