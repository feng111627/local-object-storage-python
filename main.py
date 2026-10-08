import os

import uvicorn

from object_storage.api import create_app
from object_storage.gc import GarbageCollector
from object_storage.maintenance import MaintenanceScheduler
from object_storage.models import QuotaConfig
from object_storage.retention import RetentionManager
from object_storage.store import LocalObjectStore


def _env_int(name: str) -> int | None:
    raw = os.environ.get(name)
    return int(raw) if raw else None


store = LocalObjectStore(
    os.environ.get("OBJECT_STORE_ROOT", "./data"),
    QuotaConfig(max_total_size=_env_int("OBJECT_STORE_MAX_TOTAL_SIZE")),
)
gc = GarbageCollector(
    store,
    upload_ttl=float(os.environ.get("OBJECT_STORE_UPLOAD_TTL", "3600")),
)
retention = RetentionManager(store)
interval = float(os.environ.get("OBJECT_STORE_MAINTENANCE_INTERVAL", "300"))
maintenance = MaintenanceScheduler(store, retention=retention, gc=gc, interval_seconds=interval)
app = create_app(store, gc=gc, maintenance=maintenance)


@app.on_event("startup")
def _start_maintenance() -> None:
    if os.environ.get("OBJECT_STORE_AUTO_MAINTENANCE", "1") == "1":
        maintenance.start()


@app.on_event("shutdown")
def _stop_maintenance() -> None:
    maintenance.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
