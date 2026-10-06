"""FastAPI 路由：对外 HTTP 接口。"""
from __future__ import annotations

from fastapi import APIRouter

from .store import LocalObjectStore


def create_router(store: LocalObjectStore) -> APIRouter:
    router = APIRouter()
    # TODO: 填充路由实现
    return router
