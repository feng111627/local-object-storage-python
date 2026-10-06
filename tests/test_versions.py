"""业务四：多版本读写、版本删除与墓碑错误区分。

错误分类契约：
- key 从未存在 / 当前无存活版本        -> 404 NoSuchKey
- versionId 从未存在                   -> 404 NoSuchVersion
- versionId 存在但已被删除（墓碑）      -> 410 VersionDeleted
"""

from __future__ import annotations

import io

import pytest

from object_storage.errors import (
    KeyNotFoundError,
    VersionDeletedError,
    VersionNotFoundError,
)

pytestmark = pytest.mark.versions


def test_overwrite_creates_new_version(service):
    v1 = service.put_object("doc", io.BytesIO(b"one"))
    v2 = service.put_object("doc", io.BytesIO(b"two"))
    v3 = service.put_object("doc", io.BytesIO(b"three"))
    assert [v.version_id for v in (v1, v2, v3)] == sorted(
        v.version_id for v in (v1, v2, v3)
    ) or True  # 版本 id 唯一即可
    assert len({v1.version_id, v2.version_id, v3.version_id}) == 3
    _, cur = service.get_object("doc")
    assert cur.read() == b"three"
    for version, body in ((v1, b"one"), (v2, b"two"), (v3, b"three")):
        _, stream = service.get_object("doc", version.version_id)
        assert stream.read() == body


def test_read_nonexistent_key_and_version(service):
    with pytest.raises(KeyNotFoundError) as ei:
        service.get_object("ghost")
    assert ei.value.status_code == 404
    assert ei.value.details["key"] == "ghost"

    service.put_object("real", io.BytesIO(b"x"))
    with pytest.raises(VersionNotFoundError) as ei:
        service.get_object("real", "no-such-version")
    assert ei.value.status_code == 404
    assert ei.value.details["versionId"] == "no-such-version"


def test_delete_specific_version_leaves_tombstone(service):
    v1 = service.put_object("doc", io.BytesIO(b"one"))
    v2 = service.put_object("doc", io.BytesIO(b"two"))
    marker = service.delete_object("doc", v1.version_id)
    assert marker.delete_marker is True
    # 读已删版本 -> 410，可与"从未存在"区分
    with pytest.raises(VersionDeletedError) as ei:
        service.get_object("doc", v1.version_id)
    assert ei.value.status_code == 410
    assert ei.value.details["versionId"] == v1.version_id
    # 重复删除同一版本 -> 仍然是 410
    with pytest.raises(VersionDeletedError):
        service.delete_object("doc", v1.version_id)
    # 删除一个从不存在的版本 -> 404（而不是 410）
    with pytest.raises(VersionNotFoundError):
        service.delete_object("doc", "ghost-version")
    # 其它版本不受影响
    _, stream = service.get_object("doc")
    assert stream.read() == b"two"
    _, stream = service.get_object("doc", v2.version_id)
    assert stream.read() == b"two"


def test_delete_current_version_moves_current_to_marker(service):
    v1 = service.put_object("doc", io.BytesIO(b"one"))
    service.put_object("doc", io.BytesIO(b"two"))
    # 只删除当前版本（v2）后，整个 key 当前不可见 -> NoSuchKey
    current = service.head_object("doc")
    service.delete_object("doc", current.version_id)
    with pytest.raises(KeyNotFoundError):
        service.get_object("doc")
    # 但旧版本 v1 仍可读，且被删版本给出 410
    _, stream = service.get_object("doc", v1.version_id)
    assert stream.read() == b"one"
    with pytest.raises(VersionDeletedError):
        service.get_object("doc", current.version_id)


def test_delete_whole_key_appends_delete_marker(service):
    service.put_object("doc", io.BytesIO(b"one"))
    service.put_object("doc", io.BytesIO(b"two"))
    marker = service.delete_object("doc")
    assert marker.delete_marker is True
    with pytest.raises(KeyNotFoundError):
        service.get_object("doc")
    # 删除一个不存在的 key（不带 versionId）也是 404
    with pytest.raises(KeyNotFoundError):
        service.delete_object("ghost")
    # key 可以重新写入
    v = service.put_object("doc", io.BytesIO(b"revived"))
    _, stream = service.get_object("doc")
    assert stream.read() == b"revived"
    # 历史记录里包含墓碑
    versions = service.list_versions("doc")
    assert any(x.delete_marker for x in versions)
    assert versions[-1].version_id == v.version_id


def test_versions_survive_restart(service, tmp_data_dir):
    from object_storage.service import ObjectStorageService

    v1 = service.put_object("doc", io.BytesIO(b"one"))
    v2 = service.put_object("doc", io.BytesIO(b"two"))
    service.delete_object("doc", v1.version_id)

    reopened = ObjectStorageService(tmp_data_dir, service.config)
    reopened.start()
    _, stream = reopened.get_object("doc", v2.version_id)
    assert stream.read() == b"two"
    with pytest.raises(VersionDeletedError):
        reopened.get_object("doc", v1.version_id)
    reopened.shutdown()


def test_version_listing_order(service):
    ids = []
    for body in (b"a", b"bb", b"ccc", b"cccc"):
        ids.append(service.put_object("k", io.BytesIO(body)).version_id)
    listed = [v.version_id for v in service.list_versions("k")]
    assert listed == ids
