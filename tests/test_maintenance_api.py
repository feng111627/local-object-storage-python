"""新 HTTP 契约：保留规则、版本冻结、容量、自动维护接口。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from object_storage import LocalObjectStore, QuotaConfig
from object_storage.api import create_app

logger = logging.getLogger("object_storage.tests.maintenance_api")


def err(resp) -> str:
    return resp.json()["error"]["code"]


@pytest.fixture()
def client(tmp_path):
    store = LocalObjectStore(
        tmp_path / "data", QuotaConfig(max_object_size=10_000, max_total_bytes=1000)
    )
    # auto_maintenance=False：测试里只通过接口手动触发，避免后台线程干扰。
    return TestClient(create_app(store, auto_maintenance=False), raise_server_exceptions=False)


class TestRetentionApi:
    def test_put_list_delete_rule(self, client):
        logger.info("业务覆盖：HTTP 保留规则增查删")
        r = client.put("/retention", json={"scope": "prefix", "pattern": "logs/", "max_versions": 2})
        assert r.status_code == 201 and r.json()["max_versions"] == 2
        r = client.get("/retention")
        assert r.status_code == 200 and len(r.json()["rules"]) == 1
        # 查询某 key 实际命中的规则
        r = client.get("/retention", params={"pattern": "logs/a"})
        assert r.json()["matched"]["max_versions"] == 2
        r = client.get("/retention", params={"pattern": "other"})
        assert r.json()["matched"] is None
        r = client.delete("/retention", params={"scope": "prefix", "pattern": "logs/"})
        assert r.status_code == 204
        assert len(client.get("/retention").json()["rules"]) == 0

    def test_invalid_rule_400(self, client):
        logger.info("业务覆盖：HTTP 非法规则 400")
        r = client.put("/retention", json={"scope": "key", "pattern": "k", "max_versions": 0})
        assert r.status_code == 400 and err(r) == "retention_rule_invalid"

    def test_retention_drives_expiry_over_http(self, client):
        """配规则 -> 写多版本 -> 触发维护 -> 旧版本按 version_expired 可读区分。"""
        logger.info("业务覆盖：HTTP 端到端：规则淘汰 + version_expired")
        client.put("/retention", json={"scope": "key", "pattern": "k", "max_versions": 1})
        old = client.put("/objects/k", content=b"old").json()["version_id"]
        client.put("/objects/k", content=b"new")
        r = client.post("/maintenance/run")
        assert r.status_code == 200 and r.json()["ran"] is True
        assert r.json()["stats"]["expired_versions"] == 1
        assert client.get("/objects/k").content == b"new"
        r = client.get("/objects/k", params={"version_id": old})
        assert r.status_code == 410 and err(r) == "version_expired"
        r = client.get("/objects/k", params={"version_id": "nope"})
        assert r.status_code == 404 and err(r) == "no_such_version"


class TestHoldApi:
    def test_hold_blocks_delete_and_expiry(self, client):
        logger.info("业务覆盖：HTTP 冻结版本防删除防淘汰")
        vid = client.put("/objects/k", content=b"keep").json()["version_id"]
        client.put("/retention", json={"scope": "key", "pattern": "k", "max_versions": 1})
        assert client.put(f"/objects/k/versions/{vid}/hold").status_code == 200
        # 新写一个版本，维护时旧版本因冻结而保留
        client.put("/objects/k", content=b"newer")
        client.post("/maintenance/run")
        assert client.get("/objects/k", params={"version_id": vid}).content == b"keep"
        r = client.delete("/objects/k", params={"version_id": vid})
        assert r.status_code == 409 and err(r) == "version_on_hold"
        # 解冻后可删除
        assert client.delete(f"/objects/k/versions/{vid}/hold").status_code == 200
        assert client.delete("/objects/k", params={"version_id": vid}).status_code == 200


class TestCapacityApi:
    def test_capacity_endpoint_reports_usage(self, client):
        logger.info("业务覆盖：HTTP 容量状态")
        client.put("/objects/k", content=b"x" * 100)
        r = client.get("/capacity")
        body = r.json()
        assert body["used_bytes"] == 100 and body["limit_bytes"] == 1000
        assert body["available_bytes"] == 900 and body["over_limit"] is False

    def test_total_capacity_exceeded_507(self, client):
        logger.info("业务覆盖：HTTP 总容量超限 507 且错误码可区分")
        client.put("/objects/a", content=b"a" * 600)
        r = client.put("/objects/b", content=b"b" * 500)
        assert r.status_code == 507 and err(r) == "total_capacity_exceeded"
        detail = r.json()["error"]["detail"]
        assert detail["scope"] == "put_object" and detail["limit"] == 1000

    def test_capacity_recovers_after_maintenance(self, client):
        logger.info("业务覆盖：HTTP 维护腾出空间后被拒写入恢复")
        client.put("/objects/a", content=b"a" * 900)
        r = client.put("/objects/b", content=b"b" * 200)
        assert r.status_code == 507
        vid = client.get("/objects/a/versions").json()["versions"][0]["version_id"]
        client.delete("/objects/a", params={"version_id": vid})
        client.post("/maintenance/run")
        assert client.get("/capacity").json()["used_bytes"] == 0
        assert client.put("/objects/b", content=b"b" * 200).status_code == 201


class TestMaintenanceStatusApi:
    def test_status_and_manual_run(self, client):
        logger.info("业务覆盖：HTTP 维护状态与手动触发")
        r = client.get("/maintenance/status")
        assert r.status_code == 200
        body = r.json()
        assert body["total_runs"] == 0 and body["scheduler_running"] is False
        assert "interval_seconds" in body and "capacity" in body
        r = client.post("/maintenance/run")
        assert r.json()["status"]["total_runs"] == 1
        assert r.json()["status"]["last_run_at"] is not None
        assert "reclaimed_bytes" in r.json()["stats"]
