"""FastAPI 对外接口。

接口契约（详见 README）：
- 对象读写：PUT/GET/DELETE /objects/{key}，GET /objects/{key}/versions
- 分片上传：POST /uploads，PUT /uploads/{id}/parts/{n}，
  GET /uploads/{id}/parts，POST /uploads/{id}/complete，DELETE /uploads/{id}
- 回收：POST /gc
所有业务错误返回 {"error": {"code", "message", "detail"}}，code 稳定可枚举。
"""
from __future__ import annotations

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .errors import StorageError
from .gc import GarbageCollector
from .models import UploadSession, VersionInfo
from .store import LocalObjectStore


class CompleteRequest(BaseModel):
    parts: list[int] | None = None


def _version_json(v: VersionInfo) -> dict:
    return {
        "version_id": v.version_id,
        "key": v.key,
        "size": v.size,
        "sha256": v.sha256,
        "created_at": v.created_at,
        "deleted": v.deleted,
        "delete_marker": v.delete_marker,
    }


def _session_json(s: UploadSession) -> dict:
    return {
        "upload_id": s.upload_id,
        "key": s.key,
        "created_at": s.created_at,
        "expected_size": s.expected_size,
        "expected_sha256": s.expected_sha256,
    }


def create_app(store: LocalObjectStore, gc: GarbageCollector | None = None) -> FastAPI:
    app = FastAPI(title="local-object-storage")
    collector = gc or GarbageCollector(store)

    @app.exception_handler(StorageError)
    async def storage_error_handler(_: Request, exc: StorageError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    # ---------------- 对象读写 ----------------
    @app.put("/objects/{key:path}", status_code=201)
    async def put_object(key: str, request: Request) -> dict:
        data = await request.body()
        return _version_json(store.put_object(key, data))

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

    @app.get("/objects/{key:path}/versions")
    def list_versions(key: str) -> dict:
        return {"key": key, "versions": [_version_json(v) for v in store.list_versions(key)]}

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

    @app.get("/")
    def root() -> dict:
        return {"service": "local-object-storage", "status": "ok"}

    return app
