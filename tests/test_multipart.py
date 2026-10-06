"""业务一：分片上传主流程（初始化/上传/清单/合并/读取）。"""

from __future__ import annotations

import hashlib
import io

import pytest

pytestmark = pytest.mark.multipart


def test_begin_upload_returns_session(service):
    upload = service.begin_multipart("reports/2026/q1.bin", part_size=4)
    assert upload.upload_id
    assert upload.key == "reports/2026/q1.bin"
    assert upload.part_size == 4
    assert upload.parts == {}
    assert upload.completed is False
    # 会话已持久化，可通过 get_upload 查回
    again = service.multipart.get_upload(upload.upload_id)
    assert again.upload_id == upload.upload_id


def test_upload_parts_and_list_manifest(service):
    upload = service.begin_multipart("obj", part_size=4)
    p1 = service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    p2 = service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    assert p1.size == 4 and p2.size == 2
    assert p1.checksum == hashlib.sha256(b"AAAA").hexdigest()

    parts = service.list_parts(upload.upload_id)
    assert [p.part_number for p in parts] == [1, 2]
    # 清单按分片号升序，乱序上传同样如此
    upload2 = service.begin_multipart("obj2", part_size=4)
    service.upload_part(upload2.upload_id, 3, io.BytesIO(b"CC"))
    service.upload_part(upload2.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload2.upload_id, 2, io.BytesIO(b"DD"))
    numbers = [p.part_number for p in service.list_parts(upload2.upload_id)]
    assert numbers == [1, 2, 3]


def test_complete_merges_parts_and_object_readable(service):
    upload = service.begin_multipart("video.bin", part_size=4)
    chunks = [b"AAAA", b"BBBB", b"tail"]
    for i, chunk in enumerate(chunks, start=1):
        service.upload_part(upload.upload_id, i, io.BytesIO(chunk))
    full = b"".join(chunks)
    version = service.complete_multipart(
        upload.upload_id, hashlib.sha256(full).hexdigest()
    )
    assert version.size == len(full)
    assert version.part_count == 3
    assert version.checksum == hashlib.sha256(full).hexdigest()

    record, stream = service.get_object("video.bin")
    assert stream.read() == full
    assert record.version_id == version.version_id
    # 合并成功后：活跃会话列表中不再出现该会话（completed 会话单独保留
    # 一段幂等保护窗口，由 GC 回收）
    active = service.list_uploads()
    assert all(u.upload_id != upload.upload_id for u in active)
    retained = service.multipart.list_uploads(include_completed=True)
    assert any(u.upload_id == upload.upload_id and u.completed for u in retained)


def test_complete_without_expected_checksum_uses_server_side(service):
    upload = service.begin_multipart("obj", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.upload_part(upload.upload_id, 2, io.BytesIO(b"BB"))
    version = service.complete_multipart(upload.upload_id)
    assert version.checksum == hashlib.sha256(b"AAAABB").hexdigest()


def test_complete_creates_new_version_alongside_simple_put(service):
    v1 = service.put_object("k", io.BytesIO(b"old"))
    upload = service.begin_multipart("k", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"new!"))
    v2 = service.complete_multipart(upload.upload_id)
    assert v1.version_id != v2.version_id
    _, stream = service.get_object("k")
    assert stream.read() == b"new!"
    # 旧版本仍然完整可读
    _, old = service.get_object("k", v1.version_id)
    assert old.read() == b"old"


def test_abort_upload_removes_session(service):
    upload = service.begin_multipart("k", part_size=4)
    service.upload_part(upload.upload_id, 1, io.BytesIO(b"AAAA"))
    service.abort_multipart(upload.upload_id)
    from object_storage.errors import UploadNotFoundError

    with pytest.raises(UploadNotFoundError):
        service.list_parts(upload.upload_id)
    with pytest.raises(UploadNotFoundError):
        service.abort_multipart(upload.upload_id)
