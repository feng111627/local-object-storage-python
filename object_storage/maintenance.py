"""自动维护调度器：到点自己跑回收/淘汰，并暴露可观测状态。

设计要点：
- 后台守护线程按固定间隔触发一轮 ``GarbageCollector.collect_once()``，
  也可由 ``POST /maintenance/run`` 手动触发；手动触发与定时触发互斥，
  同一时刻只有一轮在跑（跳过而非排队，避免维护任务堆积）。
- 收集的重 IO 在存储锁外执行，因此维护期间正常读写/分片上传不会被卡住。
- 状态（累计轮次、最近运行时间、上一轮统计、上一轮错误）可随时查询；
  累计轮次持久化在 ``<root>/maintenance_state.json``，重启后接着计数。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from .gc import GarbageCollector

logger = logging.getLogger("object_storage.maintenance")


class MaintenanceScheduler:
    def __init__(
        self,
        collector: GarbageCollector,
        *,
        interval_seconds: float = 300.0,
        clock=time.time,
        sleep=time.sleep,
    ):
        self.collector = collector
        self.interval_seconds = interval_seconds
        self._clock = clock
        self._sleep = sleep
        self._state_path = Path(collector.store.root) / "maintenance_state.json"
        self._lock = threading.RLock()
        self._running_lock = threading.Lock()  # 保证同一时刻只有一轮维护
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._total_runs = 0
        self._last_run_at: float | None = None
        self._last_stats: dict | None = None
        self._last_error: str | None = None
        self._last_duration_ms: float | None = None
        self._load_state()

    # ------------------------------------------------------------------
    # 持久化（仅累计轮次等轻量数据）
    # ------------------------------------------------------------------
    def _load_state(self) -> None:
        if self._state_path.exists():
            try:
                data = json.loads(self._state_path.read_text(encoding="utf-8"))
                self._total_runs = int(data.get("total_runs", 0))
                self._last_run_at = data.get("last_run_at")
            except (json.JSONDecodeError, OSError):
                pass

    def _save_state_locked(self) -> None:
        tmp = self._state_path.with_name(".maintenance_state.json.tmp")
        tmp.write_text(
            json.dumps({"total_runs": self._total_runs, "last_run_at": self._last_run_at}, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._state_path)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="object-storage-maintenance", daemon=True
            )
            self._thread.start()
            logger.info("自动维护调度器已启动，间隔 %.1fs", self.interval_seconds)

    def stop(self, timeout: float = 5.0) -> None:
        with self._lock:
            self._stop.set()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        with self._lock:
            self._thread = None

    def _loop(self) -> None:
        # 启动后先等一个间隔再跑，避免与进程启动时的请求抢资源；
        # wait 可被 stop 立即唤醒。
        while not self._stop.wait(self.interval_seconds):
            self.run_once()

    # ------------------------------------------------------------------
    # 触发与状态
    # ------------------------------------------------------------------
    def run_once(self, *, force: bool = False) -> dict | None:
        """执行一轮维护。已有一轮在跑时返回 None（跳过），否则返回本轮统计。"""
        if not force and not self._running_lock.acquire(blocking=False):
            logger.info("上一轮维护尚未结束，本轮跳过")
            return None
        elif force:
            self._running_lock.acquire(blocking=True)
        start = time.monotonic()
        try:
            stats = self.collector.collect_once()
            duration_ms = (time.monotonic() - start) * 1000
            with self._lock:
                self._total_runs += 1
                self._last_run_at = self._clock()
                self._last_stats = stats
                self._last_error = None
                self._last_duration_ms = duration_ms
                self._save_state_locked()
            logger.info(
                "维护第 %d 轮完成（%.1fms）：淘汰版本 %d，回收文件 %d，释放 %d 字节",
                self._total_runs,
                duration_ms,
                stats.get("expired_versions", 0),
                stats.get("reclaimed_version_files", 0) + stats.get("reclaimed_orphan_files", 0),
                stats.get("reclaimed_bytes", 0),
            )
            return stats
        except Exception as exc:  # 调度器绝不能因单轮异常死掉
            with self._lock:
                self._last_error = repr(exc)
            logger.exception("维护轮次失败：%s", exc)
            raise
        finally:
            self._running_lock.release()

    def status(self) -> dict:
        """可观测状态：调度配置、最近运行、累计轮次、当前容量。"""
        with self._lock:
            active = self._running_lock.locked()
            return {
                "interval_seconds": self.interval_seconds,
                "scheduler_running": self._thread is not None and self._thread.is_alive(),
                "currently_running": active,
                "total_runs": self._total_runs,
                "last_run_at": self._last_run_at,
                "last_duration_ms": self._last_duration_ms,
                "last_stats": self._last_stats,
                "last_error": self._last_error,
                "capacity": self.collector.store.capacity(),
            }
