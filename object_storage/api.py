"""FastAPI 对外接口。

接口契约（详见 README）：
- 对象读写：PUT/GET/DELETE /objects/{key}，GET /objects/{key}/versions
- 版本冻结：PUT/DELETE /objects/{key}/versions/{vid}/hold
- 分片上传：POST /uploads，PUT /uploads/{id}/parts/{n}，
  GET /uploads/{id}/parts，POST /uploads/{id}/complete，DELETE /uploads/{id}
- 保留规则：GET/PUT/DELETE /retention
- 自动维护：POST /maintenance/run，GET /maintenance/status
- 容量：GET /capacity
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
from .retention import RetentionStore
from .store import LocalObjectStore


class CompleteRequest(BaseModel):
    parts: list[int] | None = None


class RetentionRuleRequest(BaseModel):
    scope: str
    pattern: str
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
        "data_reclaimed": v.data_reclaimed,
        "expired": v.expired,
        "hold": v.hold,
    }


def _rule_json(r: RetentionRule) -> dict:
    return {
        "scope": r.scope,
        "pattern": r.pattern,
        "max_versions": r.max_versions,
        "max_age_seconds": r.max_age_seconds,
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


def create_app(
    store: LocalObjectStore,
    gc: GarbageCollector | None = None,
    *,
    retention: RetentionStore | None = None,
    scheduler: MaintenanceScheduler | None = None,
    auto_maintenance: bool = False,
    maintenance_interval: float = 300.0,
) -> FastAPI:
    app = FastAPI(title="local-object-storage")
    retention_store = retention or RetentionStore(store.root)
    collector = gc or GarbageCollector(store, retention=retention_store)
    scheduler = scheduler or MaintenanceScheduler(collector, interval_seconds=maintenance_interval)

    @app.on_event("startup")
    def _start_scheduler() -> None:
        if auto_maintenance:
            scheduler.start()

    @app.on_event("shutdown")
    def _stop_scheduler() -> None:
        scheduler.stop()

    @app.exception_handler(StorageError)
    async def storage_error_handler(_: Request, exc: StorageError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    # ---------------- 版本冻结/解冻 ----------------
    # 注意：这两条更具体的路由必须在贪婪的 /objects/{key:path} 之前注册，
    # 否则会被 PUT/DELETE /objects/{key:path} 遮蔽。
    @app.put("/objects/{key:path}/versions/{version_id}/hold")
    def hold_version(key: str, version_id: str) -> dict:
        return _version_json(store.set_version_hold(key, version_id, True))

    @app.delete("/objects/{key:path}/versions/{version_id}/hold")
    def release_hold(key: str, version_id: str) -> dict:
        return _version_json(store.set_version_hold(key, version_id, False))

    # ---------------- 对象读写 ----------------
    @app.put("/objects/{key:path}", status_code=201)
    async def put_object(key: str, request: Request) -> dict:
        data = await request.body()
        return _version_json(store.put_object(key, data))

    @app.get("/objects/{key:path}/versions")
    def list_versions(key: str) -> dict:
        return {"key": key, "versions": [_version_json(v) for v in store.list_versions(key)]}

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

    # ---------------- 保留规则 ----------------
    @app.get("/retention")
    def list_retention(pattern: str | None = Query(default=None)) -> dict:
        rules = retention_store.list_rules()
        if pattern is not None:
            matched = retention_store.rule_for(pattern)
            return {"matched": _rule_json(matched) if matched else None}
        return {"rules": [_rule_json(r) for r in rules]}

    @app.put("/retention", status_code=201)
    def put_retention(body: RetentionRuleRequest) -> dict:
        rule = retention_store.set_rule(
            body.scope,
            body.pattern,
            max_versions=body.max_versions,
            max_age_seconds=body.max_age_seconds,
        )
        return _rule_json(rule)

    @app.delete("/retention", status_code=204)
    def delete_retention(scope: str = Query(...), pattern: str = Query(...)) -> Response:
        retention_store.delete_rule(scope, pattern)
        return Response(status_code=204)

    # ---------------- 容量 ----------------
    @app.get("/capacity")
    def get_capacity() -> dict:
        return store.capacity()

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

    # ---------------- 回收 ----------------
    @app.post("/gc")
    def run_gc() -> dict:
        return collector.collect_once()

    # ---------------- 自动维护 ----------------
    @app.post("/maintenance/run")
    def run_maintenance() -> dict:
        stats = scheduler.run_once(force=True)
        return {"ran": stats is not None, "stats": stats, "status": scheduler.status()}

    @app.get("/maintenance/status")
    def maintenance_status() -> dict:
        return scheduler.status()

    @app.get("/")
    def root() -> dict:
        return {"service": "local-object-storage", "status": "ok"}

    return app
