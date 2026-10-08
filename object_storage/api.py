"""FastAPI 对外接口。

接口契约（详见 README）：
- 对象读写：PUT/GET/DELETE /objects/{key}，GET /objects/{key}/versions
- 版本保留：POST /objects/{key}/versions/{vid}/freeze|unfreeze
- 分片上传：POST /uploads，PUT /uploads/{id}/parts/{n}，
  GET /uploads/{id}/parts，POST /uploads/{id}/complete，DELETE /uploads/{id}
- 保留规则：GET/POST /retention/rules，GET/PUT/DELETE /retention/rules/{id}
- 自动维护：POST /maintenance/run，GET /maintenance/status
- 容量：GET /storage/usage
- 回收：POST /gc
所有业务错误返回 {"error": {"code", "message", "detail"}}，code 稳定可枚举。
"""
from __future__ import annotations

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .errors import StorageError
from .gc import GarbageCollector
from .maintenance import MaintenanceScheduler
from .models import RetentionRule, UploadSession, VersionInfo
from .retention import RetentionManager
from .store import LocalObjectStore


class CompleteRequest(BaseModel):
    parts: list[int] | None = None


class RetentionRuleRequest(BaseModel):
    scope: str  # "key" | "prefix"
    match: str
    max_versions: int | None = None
    max_age_seconds: float | None = None


def _version_json(v: VersionInfo) -> dict:
    return {
        "version_id": v.version_id,
        "key": v.key,
        "size": v.size,
        "sha256": v.sha256,
        "created_at": v.created_at,
        "deleted": v.deleted,
        "delete_marker": v.delete_marker,
        "expired": v.expired,
        "frozen": v.frozen,
        "data_reclaimed": v.data_reclaimed,
    }


def _rule_json(r: RetentionRule) -> dict:
    return {
        "id": r.id,
        "scope": r.scope,
        "match": r.match,
        "max_versions": r.max_versions,
        "max_age_seconds": r.max_age_seconds,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    }


def _session_json(s: UploadSession) -> dict:
    return {
        "upload_id": s.upload_id,
        "key": s.key,
        "created_at": s.created_at,
        "expected_size": s.expected_size,
        "expected_sha256": s.expected_sha256,
    }


def create_app(store: LocalObjectStore,
               gc: GarbageCollector | None = None,
               maintenance: MaintenanceScheduler | None = None) -> FastAPI:
    app = FastAPI(title="local-object-storage")
    retention = RetentionManager(store)
    collector = gc or GarbageCollector(store)
    scheduler = maintenance or MaintenanceScheduler(store, retention=retention, gc=collector)

    @app.exception_handler(StorageError)
    async def storage_error_handler(_: Request, exc: StorageError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    # ---------------- 对象读写 ----------------
    @app.put("/objects/{key:path}", status_code=201)
    async def put_object(key: str, request: Request) -> dict:
        data = await request.body()
        return _version_json(store.put_object(key, data))

    @app.get("/objects/{key:path}/versions")
    def list_versions(key: str) -> dict:
        return {"key": key, "versions": [_version_json(v) for v in store.list_versions(key)]}

    @app.post("/objects/{key:path}/versions/{version_id}/freeze", status_code=201)
    def freeze_version(key: str, version_id: str) -> dict:
        return _version_json(store.freeze_version(key, version_id))

    @app.post("/objects/{key:path}/versions/{version_id}/unfreeze", status_code=200)
    def unfreeze_version(key: str, version_id: str) -> dict:
        return _version_json(store.unfreeze_version(key, version_id))

    @app.get("/objects/{key:path}")
    def get_object(key: str, version_id: str | None = Query(default=None)) -> Response:
        info, data = store.get_object(key, version_id)
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"X-Version-Id": info.version_id, "X-Object-SHA256": info.sha256},
        )

    @app.delete("/objects/{key:path}")
    def delete_object(key: str, version_id: str | None = Query(default=None)) -> dict:
        return _version_json(store.delete_object(key, version_id))

    # ---------------- 分片上传 ----------------
    @app.post("/uploads", status_code=201)
    def create_upload(
        key: str = Query(...),
        expected_size: int | None = Query(default=None),
        expected_sha256: str | None = Query(default=None),
    ) -> dict:
        return _session_json(
            store.create_multipart_upload(key, expected_size=expected_size, expected_sha256=expected_sha256)
        )

    @app.put("/uploads/{upload_id}/parts/{part_number}")
    async def upload_part(
        upload_id: str,
        part_number: int,
        request: Request,
        x_part_sha256: str | None = Header(default=None),
    ) -> dict:
        data = await request.body()
        part = store.upload_part(upload_id, part_number, data, sha256=x_part_sha256)
        return {
            "part_number": part.part_number,
            "size": part.size,
            "sha256": part.sha256,
            "uploaded_at": part.uploaded_at,
        }

    @app.get("/uploads/{upload_id}/parts")
    def list_parts(upload_id: str) -> dict:
        return {
            "upload_id": upload_id,
            "parts": [
                {
                    "part_number": p.part_number,
                    "size": p.size,
                    "sha256": p.sha256,
                    "uploaded_at": p.uploaded_at,
                }
                for p in store.list_parts(upload_id)
            ],
        }

    @app.post("/uploads/{upload_id}/complete", status_code=201)
    def complete_upload(upload_id: str, body: CompleteRequest | None = None) -> dict:
        parts = body.parts if body else None
        return _version_json(store.complete_multipart_upload(upload_id, parts))

    @app.delete("/uploads/{upload_id}", status_code=204)
    def abort_upload(upload_id: str) -> Response:
        store.abort_multipart_upload(upload_id)
        return Response(status_code=204)

    # ---------------- 保留规则 ----------------
    @app.get("/retention/rules")
    def list_retention_rules(key: str | None = Query(default=None)) -> dict:
        if key is not None:
            rule = retention.rule_for_key(key)
            return {"key": key, "rule": _rule_json(rule) if rule else None}
        return {"rules": [_rule_json(r) for r in retention.list_rules()]}

    @app.post("/retention/rules", status_code=201)
    def create_retention_rule(body: RetentionRuleRequest) -> dict:
        rule = retention.put_rule(
            body.scope, body.match,
            max_versions=body.max_versions, max_age_seconds=body.max_age_seconds,
        )
        return _rule_json(rule)

    @app.get("/retention/rules/{rule_id}")
    def get_retention_rule(rule_id: str) -> dict:
        return _rule_json(retention.get_rule(rule_id))

    @app.put("/retention/rules/{rule_id}")
    def update_retention_rule(rule_id: str, body: RetentionRuleRequest) -> dict:
        return _rule_json(retention.put_rule(
            body.scope, body.match, rule_id=rule_id,
            max_versions=body.max_versions, max_age_seconds=body.max_age_seconds,
        ))

    @app.delete("/retention/rules/{rule_id}", status_code=204)
    def delete_retention_rule(rule_id: str) -> Response:
        retention.delete_rule(rule_id)
        return Response(status_code=204)

    # ---------------- 自动维护 ----------------
    @app.post("/maintenance/run")
    def run_maintenance() -> Response:
        result = scheduler.run_once(trigger="manual")
        if result is None:
            # 已有一轮在跑：409 可区分，调用方可用 status 轮询。
            return JSONResponse(
                status_code=409,
                content={"error": {"code": "maintenance_busy",
                                   "message": "已有一轮维护正在执行", "detail": {}}},
            )
        return JSONResponse(status_code=200, content=result)

    @app.get("/maintenance/status")
    def maintenance_status() -> dict:
        return scheduler.status()

    # ---------------- 容量 ----------------
    @app.get("/storage/usage")
    def storage_usage() -> dict:
        return store.usage()

    # ---------------- 回收（手动） ----------------
    @app.post("/gc")
    def run_gc() -> dict:
        return collector.collect_once()

    @app.get("/")
    def root() -> dict:
        return {"service": "local-object-storage", "status": "ok"}

    return app
