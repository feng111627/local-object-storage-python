"""新增能力的 HTTP 契约测试：保留规则、冻结、自动维护、总容量。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from object_storage import LocalObjectStore, QuotaConfig
from object_storage.api import create_app

logger = logging.getLogger("object_storage.tests.api_new")


def err(resp) -> str:
    return resp.json()["error"]["code"]


@pytest.fixture()
def client(tmp_path):
    store = LocalObjectStore(tmp_path / "data")
    return TestClient(create_app(store), raise_server_exceptions=False), store


@pytest.fixture()
def capped_client(tmp_path):
    store = LocalObjectStore(tmp_path / "data", QuotaConfig(max_total_size=100))
    return TestClient(create_app(store), raise_server_exceptions=False), store


class TestRetentionApi:
    def test_rule_crud_and_match(self, client):
        logger.info("业务覆盖：HTTP 保留规则增改查删")
        c, _ = client
        r = c.post("/retention/rules", json={"scope": "prefix", "match": "logs/", "max_versions": 2})
        assert r.status_code == 201
        rid = r.json()["id"]
        assert c.get("/retention/rules").json()["rules"][0]["match"] == "logs/"
        r = c.put(f"/retention/rules/{rid}", json={"scope": "prefix", "match": "logs/", "max_versions": 5})
        assert r.status_code == 200 and r.json()["max_versions"] == 5
        r = c.get("/retention/rules", params={"key": "logs/a"})
        assert r.json()["rule"]["id"] == rid
        assert c.delete(f"/retention/rules/{rid}").status_code == 204
        assert c.get(f"/retention/rules/{rid}").status_code == 404

    def test_invalid_rule_400(self, client):
        logger.info("业务覆盖：HTTP 非法规则 400")
        c, _ = client
        r = c.post("/retention/rules", json={"scope": "weird", "match": "x", "max_versions": 1})
        assert r.status_code == 400 and err(r) == "invalid_retention_rule"

    def test_freeze_protects_version_and_blocks_delete(self, client):
        logger.info("业务覆盖：HTTP 冻结版本，删除返回 409")
        c, _ = client
        vid = c.put("/objects/k", content=b"v1").json()["version_id"]
        c.put("/objects/k", content=b"v2")
        assert c.post(f"/objects/k/versions/{vid}/freeze").status_code == 201
        r = c.delete("/objects/k", params={"version_id": vid})
        assert r.status_code == 409 and err(r) == "version_protected"
        # 冻结版本仍可按版本号读
        assert c.get("/objects/k", params={"version_id": vid}).content == b"v1"
        assert c.post(f"/objects/k/versions/{vid}/unfreeze").status_code == 200


class TestMaintenanceApi:
    def test_run_and_status(self, client):
        logger.info("业务覆盖：HTTP 触发维护并查询可观测状态")
        c, store = client
        c.post("/retention/rules", json={"scope": "key", "match": "k", "max_versions": 1})
        c.put("/objects/k", content=b"old")
        c.put("/objects/k", content=b"new")
        r = c.post("/maintenance/run")
        assert r.status_code == 200
        body = r.json()
        assert body["retention"]["versions_expired"] == 1
        assert "gc" in body
        st = c.get("/maintenance/status").json()
        assert st["total_runs"] == 1
        assert st["last_finished_at"] is not None
        assert st["last_result"]["retention"]["versions_expired"] == 1

    def test_expired_version_readable_error_over_http(self, client):
        logger.info("业务覆盖：HTTP 读取淘汰版本得到 version_expired(410)，区别于 404")
        c, _ = client
        c.post("/retention/rules", json={"scope": "key", "match": "k", "max_versions": 1})
        old = c.put("/objects/k", content=b"old").json()["version_id"]
        c.put("/objects/k", content=b"new")
        c.post("/maintenance/run")
        r = c.get("/objects/k", params={"version_id": old})
        assert r.status_code == 410 and err(r) == "version_expired"
        r = c.get("/objects/k", params={"version_id": "ghost"})
        assert r.status_code == 404 and err(r) == "no_such_version"


class TestStorageUsageApi:
    def test_usage_reports_capacity(self, capped_client):
        logger.info("业务覆盖：HTTP 容量查询")
        c, _ = capped_client
        info = c.get("/storage/usage").json()
        assert info["max_total_size"] == 100
        assert info["used_bytes"] == 0
        assert info["available_bytes"] == 100

    def test_over_capacity_507(self, capped_client):
        logger.info("业务覆盖：HTTP 超总容量返回 507 storage_capacity_exceeded")
        c, _ = capped_client
        assert c.put("/objects/a", content=b"x" * 90).status_code == 201
        r = c.put("/objects/b", content=b"y" * 20)
        assert r.status_code == 507 and err(r) == "storage_capacity_exceeded"
