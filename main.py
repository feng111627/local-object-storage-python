import logging
import os

import uvicorn

from object_storage.api import create_app
from object_storage.gc import GarbageCollector
from object_storage.maintenance import MaintenanceScheduler
from object_storage.models import QuotaConfig
from object_storage.retention import RetentionStore
from object_storage.store import LocalObjectStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

root = os.environ.get("OBJECT_STORE_ROOT", "./data")

# 总容量上限（字节），默认不限；例如 OBJECT_STORE_MAX_TOTAL_BYTES=104857600
max_total = os.environ.get("OBJECT_STORE_MAX_TOTAL_BYTES")
quota = QuotaConfig(
    max_total_bytes=int(max_total) if max_total else None,
)

store = LocalObjectStore(root, quota)
retention = RetentionStore(root)
collector = GarbageCollector(store, retention=retention)

# 自动维护间隔（秒），默认 300；设为 0 可关闭自动维护。
interval = float(os.environ.get("OBJECT_STORE_MAINTENANCE_INTERVAL", "300"))
scheduler = MaintenanceScheduler(collector, interval_seconds=max(interval, 0.001))

app = create_app(store, collector, retention=retention, scheduler=scheduler, auto_maintenance=True)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
