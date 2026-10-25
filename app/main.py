import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app import callback, executor
from app.api import router
from app.config import settings
from app.db import init_db, query_one
from app.errors import ConfigError, TransitionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)
WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.ensure_dirs()
    init_db()
    executor.start()
    callback.start()
    log.info("executor started data_dir=%s", settings.data_dir)
    try:
        yield
    finally:
        callback.stop()
        executor.stop()
        log.info("executor stopped")


app = FastAPI(title="Observable AI Pipeline", version="1.0", lifespan=lifespan)
app.include_router(router)


@app.exception_handler(ConfigError)
async def config_error_handler(request: Request, exc: ConfigError):
    return JSONResponse(status_code=422, content={"detail": str(exc), "code": exc.code})


@app.exception_handler(TransitionError)
async def transition_error_handler(request: Request, exc: TransitionError):
    return JSONResponse(status_code=409, content={"detail": str(exc), "code": exc.code})


@app.get("/healthz")
def healthz():
    row = query_one("SELECT COUNT(*) AS n FROM tasks")
    return {"status": "ok", "tasks": int(row["n"])}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if WEB_DIR.exists():
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
