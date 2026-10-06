"""共享 pytest fixture 与按业务分组的日志输出。

运行测试时会先打印业务分组横幅，并在每个分组结束后汇总该组用例的
通过/失败情况，方便从日志看出覆盖了哪些业务、各用例结果如何。
"""

from __future__ import annotations

import logging

import pytest

from object_storage.config import StorageConfig
from object_storage.service import ObjectStorageService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("tests")

# marker -> 业务说明
BUSINESS_GROUPS = {
    "multipart": "业务一：分片上传主流程（初始化/上传/清单/合并/读取）",
    "resume": "业务二：断点续传、重复与并发上传同一分片的幂等性",
    "validation": "业务三：校验失败/分片缺失/分片过小/配额超限的拒绝原因",
    "versions": "业务四：多版本读写、版本删除与墓碑错误区分",
    "concurrency": "业务五：并发覆盖写的原子可见性与并发合并",
    "gc": "业务六：旧版本与过期分片回收、引用保护",
    "api": "业务七：HTTP 接口契约与错误码",
}


def pytest_configure(config):
    for marker, desc in BUSINESS_GROUPS.items():
        config.addinivalue_line("markers", f"{marker}: {desc}")


def pytest_collection_modifyitems(config, items):
    groups_seen = [
        marker for marker in BUSINESS_GROUPS
        if any(marker in item.keywords for item in items)
    ]
    reporter = config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_sep("=", f"本次测试覆盖业务分组（共 {len(groups_seen)} 组）")
        for i, marker in enumerate(groups_seen, 1):
            reporter.write_line(f"  [{i}] {BUSINESS_GROUPS[marker]}")
        reporter.write_sep("=", "开始执行")
    log.info("covered business groups: %s", groups_seen)


_group_results: dict[str, dict[str, int]] = {}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        for marker in BUSINESS_GROUPS:
            if marker in item.keywords:
                bucket = _group_results.setdefault(
                    marker, {"passed": 0, "failed": 0, "error": 0}
                )
                bucket[report.outcome] = bucket.get(report.outcome, 0) + 1


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.write_sep("=", "业务覆盖结果汇总")
    for marker, desc in BUSINESS_GROUPS.items():
        stats = _group_results.get(marker)
        if not stats:
            continue
        terminalreporter.write_line(
            f"{desc}\n    -> 通过 {stats.get('passed', 0)} / "
            f"失败 {stats.get('failed', 0)} / 错误 {stats.get('error', 0)}"
        )


@pytest.fixture
def tmp_data_dir(tmp_path):
    return str(tmp_path / "data")


@pytest.fixture
def config():
    return StorageConfig(
        min_part_size=4,
        max_object_size=1024,
        max_parts_per_upload=8,
        max_concurrent_uploads=16,
        multipart_ttl_seconds=3600,
        enable_background_gc=False,
    )


@pytest.fixture
def service(tmp_data_dir, config):
    svc = ObjectStorageService(tmp_data_dir, config)
    svc.start()
    yield svc
    svc.shutdown()
