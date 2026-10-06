"""业务七：HTTP 接口契约与错误码。"""

from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient

from object_storage.api import create_app

pytestmark = pytest.mark.api


@pytest.fixture
def client(service):
    app = create_app(service)
    with TestClient(app) as c:
        yield c


def test_root_and_health(client):
    assert client.get("/").json()["service"] == "local-object-storage"
    assert client.get("/health").json() == {"status": "ok"}


def test_put_get_head_delete_object_flow(client):
    body = b"hello-object-storage"
    r = client.put("/objects/docs/a.txt", content=body,
                   headers={"X-Checksum-Sha256": hashlib.sha256(body).hexdigest()})
    assert r.status_code == 201
    v1 = r.json()["version"]["versionId"]

    r = client.get("/objects/docs/a.txt")
    assert r.status_code == 200
    assert r.content == body
    assert r.headers["X-Version-Id"] == v1
    assert r.headers["X-Checksum-Sha256"] == hashlib.sha256(body).hexdigest()

    r = client.head("/objects/docs/a.txt")
    assert r.status_code == 200
    assert r.headers["X-Object-Size"] == str(len(body))

    # 覆盖写 -> 新版本
    r2 = client.put("/objects/docs/a.txt", content=b"v2")
    v2 = r2.json()["version"]["versionId"]
    assert v2 != v1
    assert client.get("/objects/docs/a.txt").content == b"v2"
    assert client.get(
        "/objects/docs/a.txt", params={"versionId": v1}
    ).content == body

    # 版本清单
    r = client.get("/objects/docs/a.txt/versions")
    assert [v["versionId"] for v in r.json()["versions"]] == [v1, v2]

    # 删除整个 key
    assert client.delete("/objects/docs/a.txt").status_code == 200
    assert client.get("/objects/docs/a.txt").status_code == 404


def test_error_codes_are_distinguishable(client):
    # 404 NoSuchKey
    r = client.get("/objects/ghost")
    assert r.status_code == 404 and r.json()["code"] == "NoSuchKey"
    # 404 NoSuchVersion
    client.put("/objects/k", content=b"x")
    r = client.get("/objects/k", params={"versionId": "nope"})
    assert r.status_code == 404 and r.json()["code"] == "NoSuchVersion"
    # 410 VersionDeleted
    versions = client.get("/objects/k/versions").json()["versions"]
    vid = versions[0]["versionId"]
    client.delete("/objects/k", params={"versionId": vid})
    r = client.get("/objects/k", params={"versionId": vid})
    assert r.status_code == 410 and r.json()["code"] == "VersionDeleted"


def test_multipart_upload_over_http(client):
    r = client.post("/multipart/uploads",
                    json={"key": "big.bin", "partSize": 4})
    assert r.status_code == 201
    upload = r.json()
    uid = upload["uploadId"]

    r = client.put(f"/multipart/uploads/{uid}/parts/1", content=b"AAAA",
                   headers={"X-Checksum-Sha256":
                            hashlib.sha256(b"AAAA").hexdigest()})
    assert r.status_code == 200
    etag1 = r.json()["etag"]
    # 重复上传同分片：幂等，etag 不变
    r = client.put(f"/multipart/uploads/{uid}/parts/1", content=b"AAAA")
    assert r.status_code == 200 and r.json()["etag"] == etag1

    # 同编号不同内容 -> 400 BadDigest
    r = client.put(f"/multipart/uploads/{uid}/parts/1", content=b"BBBB")
    assert r.status_code == 400 and r.json()["code"] == "BadDigest"

    client.put(f"/multipart/uploads/{uid}/parts/2", content=b"tail")
    r = client.get(f"/multipart/uploads/{uid}/parts")
    assert [p["partNumber"] for p in r.json()["parts"]] == [1, 2]

    # 缺片场景的错误码
    r2 = client.post("/multipart/uploads", json={"key": "g.bin", "partSize": 4})
    uid2 = r2.json()["uploadId"]
    client.put(f"/multipart/uploads/{uid2}/parts/1", content=b"AAAA")
    client.put(f"/multipart/uploads/{uid2}/parts/3", content=b"cc")
    r = client.post(f"/multipart/uploads/{uid2}/complete", json={})
    assert r.status_code == 400 and r.json()["code"] == "MissingPart"
    assert r.json()["details"]["missing"] == [2]

    # 坏整对象校验和
    full = b"AAAAtail"
    r = client.post(f"/multipart/uploads/{uid}/complete",
                    json={"checksumSha256": "wrong"})
    assert r.status_code == 400
    assert r.json()["code"] == "InvalidObjectChecksum"

    # 正确 checksum 完成
    r = client.post(f"/multipart/uploads/{uid}/complete",
                    json={"checksumSha256": hashlib.sha256(full).hexdigest()})
    assert r.status_code == 200
    assert client.get("/objects/big.bin").content == full

    # 重复完成请求 -> 同一版本（幂等）
    r_again = client.post(
        f"/multipart/uploads/{uid}/complete",
        json={"checksumSha256": hashlib.sha256(full).hexdigest()},
    )
    assert r_again.status_code == 200
    assert (r_again.json()["version"]["versionId"]
            == r.json()["version"]["versionId"])


def test_unknown_upload_and_abort_http(client):
    r = client.get("/multipart/uploads/nope/parts")
    assert r.status_code == 404 and r.json()["code"] == "NoSuchUpload"

    r = client.post("/multipart/uploads", json={"key": "tmp", "partSize": 4})
    uid = r.json()["uploadId"]
    client.put(f"/multipart/uploads/{uid}/parts/1", content=b"AAAA")
    assert client.delete(f"/multipart/uploads/{uid}").status_code == 200
    assert client.get(f"/multipart/uploads/{uid}").status_code == 404


def test_quota_error_http(tmp_data_dir):
    from object_storage.config import StorageConfig
    from object_storage.service import ObjectStorageService

    cfg = StorageConfig(max_concurrent_uploads=1, enable_background_gc=False)
    svc = ObjectStorageService(tmp_data_dir, cfg).start()
    client = TestClient(create_app(svc))
    client.post("/multipart/uploads", json={"key": "a", "partSize": 4})
    r = client.post("/multipart/uploads", json={"key": "b", "partSize": 4})
    assert r.status_code == 429 and r.json()["code"] == "SlowDown"
    svc.shutdown()


def test_admin_gc_endpoint(client):
    client.put("/objects/g", content=b"abc")
    r = client.post("/admin/gc")
    assert r.status_code == 200
    stats = r.json()
    assert "removedBlobs" in stats and "referencedBlobs" in stats
    assert stats["referencedBlobs"] >= 1


def test_invalid_begin_request_http(client):
    # 非 JSON
    r = client.post("/multipart/uploads", content="not-json",
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 400 and r.json()["code"] == "InvalidRequest"
    # 缺 key
    r = client.post("/multipart/uploads", json={"partSize": 4})
    assert r.status_code == 400 and r.json()["code"] == "InvalidRequest"


def test_bad_part_checksum_http(client):
    uid = client.post("/multipart/uploads",
                      json={"key": "k", "partSize": 4}).json()["uploadId"]
    r = client.put(f"/multipart/uploads/{uid}/parts/1", content=b"AAAA",
                   headers={"X-Checksum-Sha256": "0" * 64})
    assert r.status_code == 400 and r.json()["code"] == "BadDigest"
