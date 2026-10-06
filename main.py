import os

import uvicorn

from object_storage.api import create_app
from object_storage.store import LocalObjectStore

store = LocalObjectStore(os.environ.get("OBJECT_STORE_ROOT", "./data"))
app = create_app(store)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
