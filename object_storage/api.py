"""FastAPI HTTP 接口层。

错误统一转成 ``{"code", "message", "details"}`` JSON 响应，错误码
见 :mod:`object_storage.errors`。
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, Response

from .errors import StorageError
from .models import UploadRecord, VersionRecord
from .service import ObjectStorageService


def _version_dict(v: VersionRecord, include_blob: bool = False) -> dict[str, Any]:
    data = v.to_dict()
    if not include_blob:
        data.pop("blobId", None)
    return data


def _upload_dict(u: UploadRecord) -> dict[str, Any]:
    d = u.to_dict()
    # 内部字段不出现在对外契约中
    d.get("metadata", {}).pop("_completed_checksum", None)
    return d


def create_app(service: ObjectStorageService) -> FastAPI:
    app = FastAPI(
        title="Local Object Storage",
        version="0.1.0",
        description="本地磁盘模拟的分片、版本化对象存储",
    )
    app.state.service = service

    @app.exception_handler(StorageError)
    async def _storage_error_handler(request: Request, exc: StorageError):
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.to_dict(),
        )

    async def _read_body(request: Request):
        import io

        return io.BytesIO(await request.body())

    @app.get("/")
    def root():
        return {"service": "local-object-storage", "version": app.version}

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # ---------- 简单对象 ----------
    @app.put("/objects/{key:path}")
    async def put_object(
        key: str,
        request: Request,
        x_checksum_sha256: str | None = Header(default=None),
    ):
        stream = await _read_body(request)
        version = service.put_object(key, stream, checksum=x_checksum_sha256)
        return JSONResponse(
            status_code=201,
            content={"key": key, "version": _version_dict(version)},
        )

    @app.get("/objects/{key:path}/versions")
    def list_versions(key: str):
        versions = service.list_versions(key)
        return {"key": key, "versions": [_version_dict(v) for v in versions]}

    @app.get("/objects/{key:path}")
    def get_object(
        key: str,
        version_id: str | None = Query(default=None, alias="versionId"),
    ):
        version, stream = service.get_object(key, version_id)
        data = stream.read()

        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "X-Version-Id": version.version_id,
                "X-Checksum-Sha256": version.checksum,
                "X-Object-Size": str(version.size),
            },
        )

    @app.head("/objects/{key:path}")
    def head_object(
        key: str,
        version_id: str | None = Query(default=None, alias="versionId"),
    ):
        version = service.head_object(key, version_id)

        return Response(
            media_type="application/octet-stream",
            headers={
                "X-Version-Id": version.version_id,
                "X-Checksum-Sha256": version.checksum,
                "X-Object-Size": str(version.size),
            },
        )

    @app.delete("/objects/{key:path}")
    def delete_object(
        key: str,
        version_id: str | None = Query(default=None, alias="versionId"),
    ):
        marker = service.delete_object(key, version_id)
        return {"key": key, "deleteMarker": marker.to_dict()}

    # ---------- 分片上传 ----------
    @app.post("/multipart/uploads")
    async def begin_multipart(
        request: Request,
    ):
        import json as _json

        raw = await request.body()
        try:
            payload = _json.loads(raw) if raw else {}
            key = payload["key"]
        except (_json.JSONDecodeError, KeyError, TypeError):
            return JSONResponse(
                status_code=400,
                content={
                    "code": "InvalidRequest",
                    "message": "body must be JSON with a 'key' field",
                    "details": {},
                },
            )
        upload = service.begin_multipart(
            key=key,
            part_size=payload.get("partSize"),
            metadata=payload.get("metadata"),
        )
        return JSONResponse(status_code=201, content=_upload_dict(upload))

    @app.put("/multipart/uploads/{upload_id}/parts/{part_number}")
    async def upload_part(
        upload_id: str,
        part_number: int,
        request: Request,
        x_checksum_sha256: str | None = Header(default=None),
    ):
        stream = await _read_body(request)
        part = service.upload_part(
            upload_id, part_number, stream, checksum=x_checksum_sha256
        )
        return JSONResponse(status_code=200, content=part.to_dict())

    @app.get("/multipart/uploads/{upload_id}/parts")
    def list_parts(upload_id: str):
        parts = service.list_parts(upload_id)
        return {"uploadId": upload_id,
                "parts": [p.to_dict() for p in parts]}

    @app.get("/multipart/uploads/{upload_id}")
    def get_upload(upload_id: str):
        return _upload_dict(service.multipart.get_upload(upload_id))

    @app.post("/multipart/uploads/{upload_id}/complete")
    async def complete_multipart(upload_id: str, request: Request):
        import json as _json

        raw = await request.body()
        payload = _json.loads(raw) if raw else {}
        version = service.complete_multipart(
            upload_id, expected_checksum=payload.get("checksumSha256")
        )
        return {"key": version.key, "version": _version_dict(version)}

    @app.delete("/multipart/uploads/{upload_id}")
    def abort_multipart(upload_id: str):
        service.abort_multipart(upload_id)
        return {"aborted": upload_id}

    @app.get("/multipart/uploads")
    def list_uploads():
        uploads = service.list_uploads()
        return {"uploads": [_upload_dict(u) for u in uploads]}

    # ---------- 管理 ----------
    @app.post("/admin/gc")
    def run_gc():
        return service.garbage_collect()

    return app
