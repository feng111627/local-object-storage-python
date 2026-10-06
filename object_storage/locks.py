"""按键 / 按会话的锁注册表。

所有对同一 key（或同一 uploadId）的元数据修改都通过这里的锁串行化，
保证并发覆盖写、并发上传同一分片等场景下结果确定。

使用可重入锁（RLock）：同线程内可以先锁 key 再做嵌套操作而不自死锁。
注册表本身带引用计数，空闲锁会被清理，避免 key 数量无限增长。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager


class LockRegistry:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}
        self._refcount: dict[str, int] = defaultdict(int)

    @contextmanager
    def lock(self, name: str) -> Iterator[None]:
        with self._guard:
            key_lock = self._locks.get(name)
            if key_lock is None:
                key_lock = threading.RLock()
                self._locks[name] = key_lock
            self._refcount[name] += 1
        try:
            with key_lock:
                yield
        finally:
            with self._guard:
                self._refcount[name] -= 1
                if self._refcount[name] <= 0:
                    # 仅当没有线程在等待/持有时才可安全移除；
                    # RLock 被 remove 后新线程会拿到新锁。为避免与
                    # 正在获取旧锁的线程竞争，这里在 _guard 内
                    # 重新确认引用为 0 后删除（引用为 0 意味着
                    # with key_lock 的临界区均已退出）。
                    del self._locks[name]
                    del self._refcount[name]
