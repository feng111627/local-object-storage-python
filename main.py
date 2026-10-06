import os

import uvicorn
from fastapi import FastAPI

from object_storage.api import create_router
from object_storage.store import LocalObjectStore

store = LocalObjectStore(os.environ.get("OBJECT_STORE_ROOT", "./data"))

app = FastAPI(title="local-object-storage")
app.include_router(create_router(store))


@app.get("/")
def read_root():
    return {"service": "local-object-storage", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
