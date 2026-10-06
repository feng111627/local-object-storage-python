"""业务六：旧版本与过期分片回收、引用保护。

验证：
- 覆盖写后旧版本仍被引用 -> GC 不删；
- 删除版本后其 blob 成为孤儿 -> 下次 GC 回收，存活版本不受影响；
- 中止的分片上传 / 校验失败残留临时文件被清理；
- 过期未完成上传被自动中止并释放配额槽；
- sweep 可重复执行且结果稳定。
"""

from __future__ import annotations

import io
import time

import pytest

pytestmark = pytest.mark.gc


def _blob_ids_on_disk(service) -> set[str]:
    ids = set()
    for blob_id in service.gc._iter_blob_ids_on_disk():
        ids.add(blob_id)
    return ids


def test_overwritten_old_version_kept_until_deleted(service):
    v1 = service.put_object("k", io.BytesIO(b"old-version-data"))
    v2 = service.put_object("k", io.BytesIO(b"new-version-data"))
    assert service.blobs.exists(v1.blob_id)
    stats = service.garbage_collect()
    # 旧版本仍被版本元数据引用 -> 保留
    assert stats["removedBlobs"] == 0
    assert service.blobs.exists(v1.blob_id)
    assert service.blobs.exists(v2.blob_id)
    _, stream = service.get_object("k", v1.version_id)
    assert stream.read() == b"old-version-data"


def test_deleted_version_blob_reclaimed(service):
    v1 = service.put_object("k", io.BytesIO(b"old-version-data"))
    service.put_object("k", io.BytesIO(b"new-version-data"))
    old_blob = v1.blob_id
    service.delete_object("k", v1.version_id)
    # 第一次 sweep：旧 blob 成为孤儿被回收
    stats = service.garbage_collect()
    assert stats["removedBlobs"] >= 1
    assert not service.blobs.exists(old_blob)
    # 第二次 sweep：稳定，没有更多可删
    stats2 = service.garbage_collect()
    assert stats2["removedBlobs"] == 0
    # 当前版本可读
    _, stream = service.get_object("k")
    assert stream.read() == b"new-version-data"


def test_shared_blob_protected_by_any_reference(service):
    # 两个 key 写入相同内容 -> 内容寻址共享同一 blob
    a = service.put_object("a", io.BytesIO(b"shared-content"))
    b = service.put_object("b", io.BytesIO(b"shared-content"))
    assert a.blob_id == b.blob_id
    service.delete_object("a", a.version_id)
    service.garbage_collect()
    # b 仍引用该 blob，不能被删
    assert service.blobs.exists(b.blob_id)
    _, stream = service.get_object("b")
    assert stream.read() == b"shared-content"
    # 两边都删除后才回收
    service.delete_object("b", b.version_id)
    stats = service.garbage_collect()
    assert stats["removedBlobs"] >= 1
    assert not service.blobs.exists(b.blob_id)


def test_aborted_multipart_parts_reclaimed(service):
    upload = service.begin_multipart("k", part_size=4)
    part = service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    assert service.blobs.exists(part.blob_id)
    service.abort_multipart(upload.upload_id)
    stats = service.garbage_collect()
    assert not service.blobs.exists(part.blob_id)
    assert stats["removedBlobs"] >= 1


def test_expired_incomplete_upload_aborted_by_gc(service, tmp_data_dir):
    from object_storage.config import StorageConfig
    from object_storage.service import ObjectStorageService

    cfg = StorageConfig(
        min_part_size=4, multipart_ttl_seconds=0.01,
        enable_background_gc=False,
    )
    svc = ObjectStorageService(tmp_data_dir, cfg).start()
    upload = svc.begin_multipart("k", part_size=4)
    svc.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    assert svc.multipart.active_upload_count == 1
    time.sleep(0.03)
    stats = svc.garbage_collect()
    assert stats["abortedUploads"] == 1
    # 会话文件与分片都被清理，配额槽释放
    assert svc.multipart.active_upload_count == 0
    from object_storage.errors import UploadNotFoundError

    with pytest.raises(UploadNotFoundError):
        svc.list_parts(upload.upload_id)
    svc.shutdown()


def test_gc_reports_stats_and_is_serially_safe(service):
    service.put_object("k", io.BytesIO(b"hello"))
    stats = service.garbage_collect()
    for key in ("abortedUploads", "purgedCompletedSessions", "referencedBlobs",
                "removedBlobs", "keptBlobs", "removedTmpFiles", "bytesFreed",
                "elapsedSeconds"):
        assert key in stats
    assert stats["keptBlobs"] >= 1
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: service.garbage_collect(), range(8)))
    # 并发 sweep 被串行化，且每次结果一致（没有东西被误删）
    assert all(r["removedBlobs"] == 0 for r in results)
    _, stream = service.get_object("k")
    assert stream.read() == b"hello"


def test_background_gc_runs_periodically(tmp_data_dir):
    from object_storage.config import StorageConfig
    from object_storage.service import ObjectStorageService

    cfg = StorageConfig(
        min_part_size=4, multipart_ttl_seconds=0.01,
        gc_interval_seconds=0.05, enable_background_gc=True,
    )
    svc = ObjectStorageService(tmp_data_dir, cfg).start()
    upload = svc.begin_multipart("k", part_size=4)
    svc.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    deadline = time.time() + 3
    while time.time() < deadline:
        if svc.multipart.active_upload_count == 0:
            break
        time.sleep(0.05)
    assert svc.multipart.active_upload_count == 0
    svc.shutdown()
