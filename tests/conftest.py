"""测试公共配置：日志输出覆盖的业务点与各用例执行结果。"""
from __future__ import annotations

import logging
import time

import pytest

from object_storage import LocalObjectStore, QuotaConfig
from object_storage.gc import GarbageCollector

logger = logging.getLogger("object_storage.tests")


@pytest.fixture()
def store(tmp_path):
    """默认配额较宽松的业务存储实例。"""
    return LocalObjectStore(tmp_path / "data")


@pytest.fixture()
def tight_store(tmp_path):
    """配额收紧的存储实例，用于验证超限拒绝规则。"""
    return LocalObjectStore(
        tmp_path / "data",
        QuotaConfig(max_object_size=64, max_parts_per_upload=4, max_concurrent_uploads=2, max_part_size=32),
    )


@pytest.fixture()
def gc(store):
    return GarbageCollector(store, upload_ttl=0.0)


@pytest.fixture(autouse=True)
def log_test_outcome(request):
    """每个用例开始/结束时打日志，便于从日志看出覆盖的业务与结果。"""
    logger.info("▶ 开始用例 %s::%s", request.module.__name__, request.node.name)
    start = time.monotonic()
    yield
    elapsed = (time.monotonic() - start) * 1000
    logger.info("✔ 通过用例 %s (%.1fms)", request.node.name, elapsed)
