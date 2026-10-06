"""业务五：并发覆盖写的原子可见性与并发合并。

关键性质：
- 并发对同一 key 写入，读线程在任意时刻读到的都是某个完整版本，
  绝不会读到半截内容；
- 每个写请求都产生一个独立版本，版本数与成功写次数一致；
- 并发合并同一会话结果幂等，只有一个版本。
"""

from __future__ import annotations

import hashlib
import io
import random
from concurrent.futures import ThreadPoolExecutor

import pytest

pytestmark = pytest.mark.concurrency


def test_concurrent_overwrite_yields_only_complete_versions(service):
    payloads = [(i, (f"body-{i}-" + "x" * random.randint(10, 200)).encode())
                for i in range(20)]
    body_by_version: dict[str, bytes] = {}

    def write(item):
        _idx, body = item
        v = service.put_object("hot-key", io.BytesIO(body))
        body_by_version[v.version_id] = body
        return v

    allowed_bodies = {body for _, body in payloads}

    def reader():
        from object_storage.errors import KeyNotFoundError

        for _ in range(300):
            try:
                _record, stream = service.get_object("hot-key")
                data = stream.read()
            except KeyNotFoundError:
                # 第一批写入完成前可能尚无 key，容忍这一种可预期情况
                continue
            # 核心断言：读到的内容必须完整等于某个已提交版本
            assert data in body_by_version.values() or data in allowed_bodies

    with ThreadPoolExecutor(max_workers=8) as pool:
        rfut = [pool.submit(reader) for _ in range(4)]
        wfut = [pool.submit(write, p) for p in payloads]
        for f in rfut:
            f.result()  # 任一断言失败都会在此抛出
        versions = [f.result() for f in wfut]

    # 20 次成功写 -> 20 个版本（无丢失、无合并）
    assert len({v.version_id for v in versions}) == 20
    listed = service.list_versions("hot-key")
    assert len(listed) == 20
    # 最终内容是某次完整写
    _, stream = service.get_object("hot-key")
    assert stream.read() in {b for _, b in payloads}


def test_concurrent_part_uploads_distinct_numbers(tmp_data_dir):
    from object_storage.config import StorageConfig
    from object_storage.service import ObjectStorageService

    cfg = StorageConfig(
        min_part_size=64, max_parts_per_upload=32,
        max_concurrent_uploads=4, max_object_size=10_000_000,
        enable_background_gc=False,
    )
    service = ObjectStorageService(tmp_data_dir, cfg).start()
    upload = service.begin_multipart("big", part_size=64)
    n = 16

    def upload_one(i):
        body = (f"part-{i:02d}:".encode().ljust(64, b"y"))
        part = service.upload_part(
            upload.upload_id, i, io.BytesIO(body),
            checksum=hashlib.sha256(body).hexdigest(),
        )
        return part

    with ThreadPoolExecutor(max_workers=8) as pool:
        parts = list(pool.map(upload_one, range(1, n + 1)))
    assert len({p.part_number for p in parts}) == n
    assert sorted(p.part_number for p in parts) == list(range(1, n + 1))
    listed = service.list_parts(upload.upload_id)
    assert len(listed) == n
    version = service.complete_multipart(upload.upload_id)
    assert version.size == 64 * n
    service.shutdown()


def test_concurrent_complete_requests_commit_single_version(service):
    upload = service.begin_multipart("k", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    expected = hashlib.sha256(b"AAAABB").hexdigest()

    def complete(_):
        return service.complete_multipart(upload.upload_id, expected)

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(complete, range(10)))
    assert len({v.version_id for v in results}) == 1
    assert len(service.list_versions("k")) == 1


def test_concurrent_simple_put_and_multipart_complete_linearized(service):
    """简单写与分片合并并发指向同一 key：所有版本都完整可读。"""
    upload = service.begin_multipart("k", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"MLT-"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"data"))
    expected = hashlib.sha256(b"MLT-data").hexdigest()

    outcomes = []

    def multipart_commit():
        outcomes.append(("m", service.complete_multipart(upload.upload_id,
                                                         expected).version_id))

    def simple_put(i):
        body = f"simple-{i}".encode()
        outcomes.append(("s", service.put_object("k", io.BytesIO(body)).version_id,
                         body))

    with ThreadPoolExecutor(max_workers=6) as pool:
        f1 = pool.submit(multipart_commit)
        f2 = [pool.submit(simple_put, i) for i in range(8)]
        f1.result()
        [f.result() for f in f2]

    versions = service.list_versions("k")
    assert len(versions) == 9  # 1 分片 + 8 简单
    # 每个版本的 blob 都能完整读回且校验通过
    for v in versions:
        _, stream = service.get_object("k", v.version_id)
        data = stream.read()
        assert hashlib.sha256(data).hexdigest() == v.checksum
