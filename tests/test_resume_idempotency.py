"""业务二：断点续传、重复与并发上传同一分片的幂等性。"""

from __future__ import annotations

import hashlib
import io
from concurrent.futures import ThreadPoolExecutor

import pytest

from object_storage.service import ObjectStorageService

pytestmark = pytest.mark.resume


def test_resume_after_simulated_interruption(service):
    """上传部分分片后进程"中断"：用一个新的 service 指向同一数据目录，
    已上传分片仍在清单中，补齐即可完成合并（断点续传）。"""
    upload = service.begin_multipart("big.bin", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BBBB"))

    # 模拟重启：新建 service 实例（不上传新数据）
    resumed = ObjectStorageService(service.data_dir, service.config)
    resumed.start()
    parts = resumed.list_parts(upload.upload_id)
    assert [p.part_number for p in parts] == [1, 2]
    # 补传缺失的第 3 片
    resumed.upload_part(upload.upload_id, 3, io.BytesIO(b"end"))
    full = b"AAAABBBBend"
    version = resumed.complete_multipart(
        upload.upload_id, hashlib.sha256(full).hexdigest()
    )
    _, stream = resumed.get_object("big.bin")
    assert stream.read() == full
    assert version.part_count == 3


def test_duplicate_part_same_content_is_idempotent(service):
    upload = service.begin_multipart("k", part_size=4)
    r1 = service.upload_part(
        upload.upload_id, 1, io.BytesIO(b"AAAA"),
        checksum=hashlib.sha256(b"AAAA").hexdigest(),
    )
    # 完全相同的字节重复上传 -> 返回同一条记录
    r2 = service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    assert r1.blob_id == r2.blob_id
    assert r1.uploaded_at == r2.uploaded_at
    # 分片只计数一次，合并结果正确
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    version = service.complete_multipart(upload.upload_id)
    assert version.size == 6


def test_concurrent_same_part_upload_wins_once(service):
    upload = service.begin_multipart("k", part_size=4)

    def push(_):
        return service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(push, range(16)))
    blob_ids = {r.blob_id for r in results}
    assert blob_ids == {hashlib.sha256(b"AAAA").hexdigest()}
    assert len(service.list_parts(upload.upload_id)) == 1


def test_concurrent_same_part_different_content_rejected(service):
    upload = service.begin_multipart("k", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    from object_storage.errors import PartChecksumMismatchError

    def push(data):
        return service.upload_part(upload.upload_id, 1, io.BytesIO(data))

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(push, b"AAAA"), pool.submit(push, b"ZZZZ"),
                   pool.submit(push, b"AAAA"), pool.submit(push, b"????")]
        outcomes = []
        for f in futures:
            try:
                outcomes.append(("ok", f.result()))
            except PartChecksumMismatchError:
                outcomes.append(("reject", None))
    # 首写内容 AAAA 保持不变；不同内容全部被明确拒绝
    kept = service.list_parts(upload.upload_id)[0]
    assert kept.checksum == hashlib.sha256(b"AAAA").hexdigest()
    assert any(o[0] == "reject" for o in outcomes)


def test_retry_part_after_checksum_failure_then_succeeds(service):
    upload = service.begin_multipart("k", part_size=4)
    from object_storage.errors import PartChecksumMismatchError

    # 客户端声明了错误的 checksum：服务端拒绝且不落盘脏数据
    with pytest.raises(PartChecksumMismatchError):
        service.upload_part(
            upload.upload_id, 1, io.BytesIO(b"AAAA"),
            checksum="0" * 64,
        )
    # 用正确 checksum 重试成功（前一次失败不影响会话）
    service.upload_part(
        upload.upload_id, 1, io.BytesIO(b"AAAA"),
        checksum=hashlib.sha256(b"AAAA").hexdigest(),
    )
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    version = service.complete_multipart(upload.upload_id)
    assert version.size == 6


def test_complete_is_idempotent_under_duplicate_requests(service):
    upload = service.begin_multipart("k", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    v1 = service.complete_multipart(upload.upload_id)
    v2 = service.complete_multipart(upload.upload_id)
    # 重复合并请求不会产生第二个版本
    assert v1.version_id == v2.version_id
    versions = service.list_versions("k")
    assert len(versions) == 1

