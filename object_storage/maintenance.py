"""自动维护：定时把「保留规则淘汰 + 空间回收」自己跑起来，并暴露运行状态。

可观测性（状态持久化在 ``root/state/maintenance.json``，重启不丢）：
- ``total_runs``：累计完成的轮次；
- ``last_started_at`` / ``last_finished_at``：最近一轮的起止时间；
- ``last_result``：最近一轮清掉了什么（淘汰版本数、回收字节数等）；
- ``last_error``：最近一轮的失败信息（运行异常不会杀死后台线程）；
- ``running`` / ``scheduler_running``：当前是否有一轮在跑、后台线程是否在运转。

并发安全：
- 后台线程与手动触发共用同一把运行锁，同一时刻最多一轮维护；
- 维护内部逐 key 加锁，与其它 key 的正常读写、分片上传完全并发；
- 清理数据前有 read pin 检查，不会删掉正在读取的数据。
"""
from __future__ import annotations

import json
import threading
import time

from .gc import GarbageCollector
from .retention import RetentionManager


class MaintenanceScheduler:
    """后台定时维护线程 + 持久化运行状态。"""

    def __init__(self, store,
                 retention: RetentionManager | None = None,
                 gc: GarbageCollector | None = None,
                 *, interval_seconds: float = 300.0):
        self.store = store
        self.retention = retention or RetentionManager(store)
        self.gc = gc or GarbageCollector(store)
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._run_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._currently_running = False
        self._status_lock = threading.Lock()
        self._state_path = self.store.root / "state" / "maintenance.json"

    # ------------------------------------------------------------------
    # 状态持久化
    # ------------------------------------------------------------------
    def _load_state(self) -> dict:
        if not self._state_path.exists():
            return {"total_runs": 0, "last_started_at": None, "last_finished_at": None,
                    "last_result": None, "last_error": None, "last_trigger": None}
        try:
            return json.loads(self._state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"total_runs": 0, "last_started_at": None, "last_finished_at": None,
                    "last_result": None, "last_error": None, "last_trigger": None}

    def _save_state(self, state: dict) -> None:
        self.store._atomic_write(
            self._state_path, json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
        )

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------
    def run_once(self, *, trigger: str = "manual") -> dict | None:
        """同步执行一轮维护（先淘汰、后回收）。

        已有一轮在跑时返回 ``None``（不重叠、不排队）；否则返回合并后的统计。
        """
        if not self._run_lock.acquire(blocking=False):
            return None
        with self._status_lock:
            self._currently_running = True
            state = self._load_state()
            state["last_started_at"] = time.time()
            state["last_trigger"] = trigger
            self._save_state(state)
        try:
            retention_stats = self.retention.apply_once()
            gc_stats = self.gc.collect_once()
            result = {"retention": retention_stats, "gc": gc_stats, "trigger": trigger}
            with self._status_lock:
                state = self._load_state()
                state["total_runs"] += 1
                state["last_finished_at"] = time.time()
                state["last_result"] = result
                state["last_error"] = None
                self._save_state(state)
            return result
        except BaseException as exc:  # 维护失败不影响存储主流程，也不能杀死后台线程
            with self._status_lock:
                state = self._load_state()
                state["last_finished_at"] = time.time()
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                self._save_state(state)
            raise
        finally:
            with self._status_lock:
                self._currently_running = False
            self._run_lock.release()

    # ------------------------------------------------------------------
    # 后台线程
    # ------------------------------------------------------------------
    def start(self) -> None:
        """启动后台定时维护线程（重复调用安全）。"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()

        def _loop() -> None:
            # 启动后先等一个间隔，避免和服务启动初始化抢资源；也可在等待中被快速停止。
            while not self._stop.wait(self.interval_seconds):
                try:
                    self.run_once(trigger="scheduled")
                except Exception:
                    # 错误已记入状态，继续下一轮
                    pass

        self._thread = threading.Thread(target=_loop, name="maintenance-scheduler", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=timeout)
        self._thread = None

    @property
    def running(self) -> bool:
        with self._status_lock:
            return self._currently_running

    @property
    def scheduler_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict:
        """返回当前运行状态（重启后累计轮次/最近结果仍在）。"""
        with self._status_lock:
            state = self._load_state()
            return {
                "running": self._currently_running,
                "scheduler_running": self.scheduler_running,
                "interval_seconds": self.interval_seconds,
                "total_runs": state["total_runs"],
                "last_started_at": state["last_started_at"],
                "last_finished_at": state["last_finished_at"],
                "last_trigger": state.get("last_trigger"),
                "last_result": state["last_result"],
                "last_error": state.get("last_error"),
            }
