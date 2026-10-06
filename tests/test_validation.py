"""业务三：校验失败/分片缺失/分片过小/配额超限的拒绝原因。

每个用例都断言具体的错误码与 details，保证"拒绝原因可区分"，
而不是笼统地失败。
"""

from __future__ import annotations

import hashlib
import io

import pytest

from object_storage.errors import (
    InvalidPartNumberError,
    MissingPartError,
    ObjectChecksumMismatchError,
    ObjectTooLargeError,
    PartChecksumMismatchError,
    PartTooSmallError,
    TooManyPartsError,
    TooManyUploadsError,
    UploadAlreadyCompletedError,
    UploadNotFoundError,
)

pytestmark = pytest.mark.validation


def _begin(service, key="k", part_size=4):
    return service.begin_multipart(key, part_size=part_size)


def test_unknown_upload_is_rejected(service):
    with pytest.raises(UploadNotFoundError) as ei:
        service.list_parts("no-such-upload")
    assert ei.value.code == "NoSuchUpload"
    assert ei.value.status_code == 404


def test_invalid_part_numbers(service):
    upload = _begin(service)
    for bad in (0, -1, service.config.max_parts_per_upload + 1):
        with pytest.raises(InvalidPartNumberError) as ei:
            service.upload_part(upload.upload_id, bad, io.BytesIO(b"x"))
        assert ei.value.details["partNumber"] == bad


def test_bad_part_checksum_rejected_and_not_committed(service):
    upload = _begin(service)
    with pytest.raises(PartChecksumMismatchError) as ei:
        service.upload_part(
            upload.upload_id, 1, io.BytesIO(b"AAAA"), checksum="f" * 64
        )
    err = ei.value
    assert err.details["expected"] == "f" * 64
    assert err.details["actual"] == hashlib.sha256(b"AAAA").hexdigest()
    # 脏数据没有进入清单
    assert service.list_parts(upload.upload_id) == []


def test_missing_middle_part_rejected_with_gap_list(service):
    upload = _begin(service)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 3, io.BytesIO(b"cc"))
    with pytest.raises(MissingPartError) as ei:
        service.complete_multipart(upload.upload_id)
    assert ei.value.details["missing"] == [2]
    # 未提交任何版本
    from object_storage.errors import KeyNotFoundError

    with pytest.raises(KeyNotFoundError):
        service.head_object("k")


def test_no_parts_complete_rejected(service):
    upload = _begin(service)
    with pytest.raises(MissingPartError):
        service.complete_multipart(upload.upload_id)


def test_non_last_part_too_small_rejected(service):
    upload = _begin(service, part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AA"))  # 非末片过短
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    with pytest.raises(PartTooSmallError) as ei:
        service.complete_multipart(upload.upload_id)
    assert ei.value.details["partNumber"] == 1
    assert ei.value.details["partSize"] == 4


def test_last_part_may_be_smaller(service):
    upload = _begin(service, part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"x"))  # 末片允许短
    version = service.complete_multipart(upload.upload_id)
    assert version.size == 5


def test_merged_object_checksum_mismatch_rejected(service):
    upload = _begin(service)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    with pytest.raises(ObjectChecksumMismatchError) as ei:
        service.complete_multipart(upload.upload_id, "deadbeef")
    assert ei.value.details["actual"] == hashlib.sha256(b"AAAABB").hexdigest()
    # 失败的合并不产生版本；修正 checksum 后仍可成功（会话完好）
    with pytest.raises(Exception):  # noqa: B017
        service.head_object("k")
    version = service.complete_multipart(
        upload.upload_id, hashlib.sha256(b"AAAABB").hexdigest()
    )
    assert version.size == 6


def test_object_total_size_quota(service):
    upload = _begin(service, part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"A" * 600))
    with pytest.raises(ObjectTooLargeError) as ei:
        service.upload_part(upload.upload_id, 2, io.BytesIO(b"B" * 600))
    assert ei.value.details["limit"] == service.config.max_object_size
    assert ei.value.details["total"] > service.config.max_object_size


def test_simple_put_size_quota(service):
    with pytest.raises(ObjectTooLargeError):
        service.put_object("big", io.BytesIO(b"x" * 5000))


def test_parts_count_quota(tmp_data_dir):
    from object_storage.config import StorageConfig
    from object_storage.service import ObjectStorageService

    # 分片号合法范围 = max_parts_per_upload，因此上限设 3 时：
    # 1..3 可传，第 4 片因数量/编号上限被拒。
    cfg = StorageConfig(
        min_part_size=1, max_parts_per_upload=3, max_concurrent_uploads=4,
        max_object_size=10_000, enable_background_gc=False,
    )
    svc = ObjectStorageService(tmp_data_dir, cfg).start()
    upload = svc.begin_multipart("k", part_size=1)
    svc.upload_part(upload.upload_id, 1, io.BytesIO(b"a"))
    svc.upload_part(upload.upload_id, 2, io.BytesIO(b"b"))
    svc.upload_part(upload.upload_id, 3, io.BytesIO(b"c"))
    with pytest.raises((TooManyPartsError, InvalidPartNumberError)) as ei:
        svc.upload_part(upload.upload_id, 4, io.BytesIO(b"d"))
    assert ei.value.status_code == 400
    svc.shutdown()


def test_concurrent_uploads_quota(tmp_data_dir):
    from object_storage.config import StorageConfig
    from object_storage.service import ObjectStorageService

    cfg = StorageConfig(
        min_part_size=4, max_concurrent_uploads=2, enable_background_gc=False,
    )
    svc = ObjectStorageService(tmp_data_dir, cfg).start()
    first = svc.begin_multipart("a")
    svc.begin_multipart("b")
    with pytest.raises(TooManyUploadsError) as ei:
        svc.begin_multipart("c")
    assert ei.value.details["limit"] == 2
    assert ei.value.details["active"] == 2
    # 中止一个会话后槽位释放，可以再建
    svc.abort_multipart(first.upload_id)
    third = svc.begin_multipart("c")
    assert third.upload_id
    # 合并完成也会释放槽位
    svc.upload_part(third.upload_id, 1, io.BytesIO(b"AAAA"))
    svc.complete_multipart(third.upload_id)
    assert svc.multipart.active_upload_count == 1
    svc.shutdown()


def test_part_upload_after_completion_rejected(service):
    upload = _begin(service)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.complete_multipart(upload.upload_id)
    with pytest.raises(UploadAlreadyCompletedError):
        service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    with pytest.raises(UploadAlreadyCompletedError):
        service.abort_multipart(upload.upload_id)
