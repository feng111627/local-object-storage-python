"""HTTP 接口测试：对外契约、错误分类、分片上传全流程。"""
from __future__ import annotations

import hashlib
import logging

import pytest
from fastapi.testclient import TestClient

from object_storage import LocalObjectStore, QuotaConfig
from object_storage.api import create_app

logger = logging.getLogger("object_storage.tests.api")


@pytest.fixture()
def client(tmp_path):
    store = LocalObjectStore(tmp_path / "data", QuotaConfig(max_object_size=64, max_concurrent_uploads=2))
    return TestClient(create_app(store), raise_server_exceptions=False)


def err(resp) -> str:
    return resp.json()["error"]["code"]


class TestObjectApi:
    def test_put_get_roundtrip(self, client):
        """PUT 创建版本，GET 读回内容并带版本响应头。"""
        logger.info("业务覆盖：HTTP 对象读写契约")
        r = client.put("/objects/a/b.txt", content=b"hello")
        assert r.status_code == 201
        vid = r.json()["version_id"]
        r = client.get("/objects/a/b.txt")
        assert r.status_code == 200 and r.content == b"hello"
        assert r.headers["x-version-id"] == vid
        r = client.get("/objects/a/b.txt", params={"version_id": vid})
        assert r.content == b"hello"

    def test_get_missing_key_404(self, client):
        logger.info("业务覆盖：HTTP 404 no_such_key")
        r = client.get("/objects/nope")
        assert r.status_code == 404 and err(r) == "no_such_key"

    def test_version_error_classification(self, client):
        """读已删除版本 410、读不存在版本 404，错误码可区分。"""
        logger.info("业务覆盖：HTTP 版本错误分类（410 vs 404）")
        vid = client.put("/objects/k", content=b"x").json()["version_id"]
        client.delete("/objects/k", params={"version_id": vid})
        r = client.get("/objects/k", params={"version_id": vid})
        assert r.status_code == 410 and err(r) == "version_already_deleted"
        r = client.get("/objects/k", params={"version_id": "nonexistent"})
        assert r.status_code == 404 and err(r) == "no_such_version"

    def test_list_versions(self, client):
        logger.info("业务覆盖：HTTP 版本清单")
        client.put("/objects/k", content=b"a")
        client.put("/objects/k", content=b"b")
        r = client.get("/objects/k/versions")
        assert r.status_code == 200
        assert len(r.json()["versions"]) == 2

    def test_quota_exceeded_413(self, client):
        logger.info("业务覆盖：HTTP 配额超限 413")
        r = client.put("/objects/big", content=b"x" * 65)
        assert r.status_code == 413 and err(r) == "object_too_large"


class TestMultipartApi:
    def test_full_multipart_flow_over_http(self, client):
        """HTTP 全流程：建会话→传分片→查清单→合并→读回。"""
        logger.info("业务覆盖：HTTP 分片上传全流程")
        r = client.post("/uploads", params={"key": "big", "expected_size": 6})
        assert r.status_code == 201
        uid = r.json()["upload_id"]
        assert client.put(f"/uploads/{uid}/parts/1", content=b"foo").status_code == 200
        assert client.put(f"/uploads/{uid}/parts/2", content=b"bar").status_code == 200
        parts = client.get(f"/uploads/{uid}/parts").json()["parts"]
        assert [p["part_number"] for p in parts] == [1, 2]
        r = client.post(f"/uploads/{uid}/complete", json={})
        assert r.status_code == 201
        assert client.get("/objects/big").content == b"foobar"

    def test_part_checksum_header_enforced(self, client):
        logger.info("业务覆盖：HTTP 分片校验头")
        uid = client.post("/uploads", params={"key": "k"}).json()["upload_id"]
        r = client.put(f"/uploads/{uid}/parts/1", content=b"data", headers={"X-Part-SHA256": "0" * 64})
        assert r.status_code == 400 and err(r) == "part_checksum_mismatch"
        good = hashlib.sha256(b"data").hexdigest()
        r = client.put(f"/uploads/{uid}/parts/1", content=b"data", headers={"X-Part-SHA256": good})
        assert r.status_code == 200

    def test_complete_missing_parts_400(self, client):
        logger.info("业务覆盖：HTTP 合并缺片 400")
        uid = client.post("/uploads", params={"key": "k"}).json()["upload_id"]
        client.put(f"/uploads/{uid}/parts/1", content=b"a")
        r = client.post(f"/uploads/{uid}/complete", json={"parts": [1, 2]})
        assert r.status_code == 400 and err(r) == "missing_parts"
        assert r.json()["error"]["detail"]["missing"] == [2]

    def test_part_conflict_409(self, client):
        logger.info("业务覆盖：HTTP 分片冲突 409")
        uid = client.post("/uploads", params={"key": "k"}).json()["upload_id"]
        client.put(f"/uploads/{uid}/parts/1", content=b"a")
        r = client.put(f"/uploads/{uid}/parts/1", content=b"b")
        assert r.status_code == 409 and err(r) == "part_conflict"
        # 幂等重传相同内容 → 200
        r = client.put(f"/uploads/{uid}/parts/1", content=b"a")
        assert r.status_code == 200

    def test_concurrent_upload_quota_429(self, client):
        logger.info("业务覆盖：HTTP 并发上传配额 429")
        client.post("/uploads", params={"key": "k"})
        client.post("/uploads", params={"key": "k"})
        r = client.post("/uploads", params={"key": "k"})
        assert r.status_code == 429 and err(r) == "too_many_concurrent_uploads"

    def test_unknown_upload_404(self, client):
        logger.info("业务覆盖：HTTP 不存在会话 404")
        r = client.get("/uploads/nope/parts")
        assert r.status_code == 404 and err(r) == "no_such_upload"

    def test_abort_upload(self, client):
        logger.info("业务覆盖：HTTP 中止会话")
        uid = client.post("/uploads", params={"key": "k"}).json()["upload_id"]
        assert client.delete(f"/uploads/{uid}").status_code == 204
        assert client.get(f"/uploads/{uid}/parts").status_code == 404

    def test_gc_endpoint(self, client):
        logger.info("业务覆盖：HTTP 触发回收")
        r = client.post("/gc")
        assert r.status_code == 200
        assert "reclaimed_bytes" in r.json()
