"""自动维护调度测试：定时触发、状态可观测、重启累计、与业务并发安全。"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from object_storage.errors import NoSuchKey, VersionExpired
from object_storage.gc import GarbageCollector
from object_storage.maintenance import MaintenanceScheduler
from object_storage.retention import RetentionManager

logger = logging.getLogger("object_storage.tests.maintenance")


class TestSchedulerObservability:
    def test_run_once_returns_retention_and_gc_stats(self, store):
        logger.info("业务覆盖：单轮维护返回淘汰与回收两部分统计")
        rm = RetentionManager(store)
        rm.put_rule("key", "k", max_versions=1)
        store.put_object("k", b"old")
        store.put_object("k", b"newlong")
        sch = MaintenanceScheduler(store, retention=rm, gc=GarbageCollector(store))
        result = sch.run_once()
        assert result["retention"]["versions_expired"] == 1
        assert result["gc"]["reclaimed_bytes"] == 3
        st = sch.status()
        assert st["total_runs"] == 1
        assert st["last_result"] == result
        assert st["last_started_at"] is not None
        assert st["last_finished_at"] is not None
        assert st["running"] is False
        assert st["last_error"] is None

    def test_status_persists_across_restart(self, store):
        logger.info("业务覆盖：累计轮次与最近结果重启后仍可查")
        sch = MaintenanceScheduler(store)
        for _ in range(3):
            sch.run_once()
        reborn = MaintenanceScheduler(store)
        st = reborn.status()
        assert st["total_runs"] == 3
        assert st["last_result"] is not None

    def test_scheduled_runs_happen_automatically(self, store):
        logger.info("业务覆盖：到点自动跑，不用手动触发")
        rm = RetentionManager(store)
        rm.put_rule("key", "k", max_versions=1)
        old = store.put_object("k", b"old12")
        store.put_object("k", b"new")
        sch = MaintenanceScheduler(store, retention=rm, interval_seconds=0.05)
        sch.start()
        try:
            deadline = time.time() + 5
            while time.time() < deadline:
                if sch.status()["total_runs"] >= 1:
                    break
                time.sleep(0.02)
            assert sch.status()["total_runs"] >= 1
            # 旧版本被自动淘汰
            with pytest.raises(VersionExpired):
                store.get_object("k", old.version_id)
        finally:
            sch.stop()
        assert sch.scheduler_running is False

    def test_manual_and_scheduled_do_not_overlap(self, store, monkeypatch):
        logger.info("业务覆盖：同一时刻最多一轮维护，重复触发返回忙")
        sch = MaintenanceScheduler(store, interval_seconds=10)
        entered = threading.Event()
        release = threading.Event()
        active_count = {"n": 0}
        max_active = {"n": 0}
        lock = threading.Lock()

        def slow_apply(*a, **k):
            with lock:
                active_count["n"] += 1
                max_active["n"] = max(max_active["n"], active_count["n"])
            entered.set()
            release.wait(timeout=5)
            with lock:
                active_count["n"] -= 1
            return {"versions_expired": 0, "keys_scanned": 0, "expired_bytes": 0, "rules_applied": 0}

        monkeypatch.setattr(sch.retention, "apply_once", slow_apply)
        t = threading.Thread(target=sch.run_once)
        t.start()
        entered.wait(timeout=5)
        # 维护进行中：再次手动触发拿到 None（忙），不重叠执行
        assert sch.run_once() is None
        assert sch.running is True
        release.set()
        t.join()
        assert max_active["n"] == 1
        assert sch.run_once()["retention"]["keys_scanned"] == 0


class TestConcurrentMaintenance:
    def test_maintenance_concurrent_with_writes_and_reads(self, store):
        logger.info("业务覆盖：维护与持续读写/分片并发，不卡正常流量且结果一致")
        rm = RetentionManager(store)
        rm.put_rule("prefix", "k", max_versions=3)
        sch = MaintenanceScheduler(store, retention=rm, interval_seconds=0.01)
        stop = threading.Event()
        errors: list[Exception] = []

        def writer():
            i = 0
            while not stop.is_set():
                try:
                    store.put_object(f"k{i % 5}", f"payload-{i}".encode())
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
                i += 1

        def reader():
            while not stop.is_set():
                for k in range(5):
                    try:
                        _, data = store.get_object(f"k{k}")
                        assert data.startswith(b"payload-")
                    except Exception:
                        pass  # key 尚未创建/被淘汰窗口等可接受

        threads = [threading.Thread(target=writer) for _ in range(3)]
        threads.append(threading.Thread(target=reader))
        for t in threads:
            t.start()
        sch.start()
        time.sleep(1.0)
        sch.stop()
        stop.set()
        for t in threads:
            t.join(timeout=5)
        assert not errors, errors[:3]
        # 维护确实跑了多轮
        assert sch.status()["total_runs"] >= 2
        # 停止写入后再跑一轮：状态应收敛到保留规则之内（最终一致）
        sch.run_once()
        sch.gc.collect_once()
        for k in range(5):
            live = [v for v in store.list_versions(f"k{k}")
                    if not v.deleted and not v.expired and not v.delete_marker]
            assert len(live) <= 3
        # 最新版本始终可读
        for k in range(5):
            assert store.get_object(f"k{k}")[1].startswith(b"payload-")

    def test_unrelated_keys_not_blocked_by_maintenance_on_busy_key(self, store):
        logger.info("业务覆盖：逐 key 加锁，维护一个 key 不阻塞其它 key 的读写")
        sch = MaintenanceScheduler(store)
        blocking_lock = store._key_locks.setdefault("busy", threading.Lock())
        blocking_lock.acquire()  # 占住 busy key
        done = threading.Event()

        def maintain():
            sch.run_once()
            done.set()

        t = threading.Thread(target=maintain)
        t.start()
        # busy 被占住期间，其它 key 的读写应立即成功
        store.put_object("other", b"free")
        assert store.get_object("other")[1] == b"free"
        blocking_lock.release()
        assert done.wait(timeout=5)
