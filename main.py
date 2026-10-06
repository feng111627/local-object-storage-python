import os

import uvicorn

from object_storage.api import create_app
from object_storage.service import ObjectStorageService

DATA_DIR = os.environ.get("OBJECT_STORAGE_DIR", "./.local-object-storage")

service = ObjectStorageService(DATA_DIR).start()
app = create_app(service)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
