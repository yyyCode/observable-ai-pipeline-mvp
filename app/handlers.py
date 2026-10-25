import json
import threading
import time
from dataclasses import dataclass
from typing import Callable

from app.errors import CanceledError

HANDLER_VERSION = "1"


@dataclass
class HandlerContext:
    step_name: str
    config: dict
    params: dict
    attempt_no: int
    cancel_event: threading.Event
    log: Callable[[dict], None]


def _sleep(ctx: HandlerContext, default: float) -> None:
    seconds = float(ctx.params.get("slow_steps", {}).get(ctx.step_name, default))
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if ctx.cancel_event.is_set():
            raise CanceledError("CANCELED", "canceled during sleep")
        time.sleep(min(0.1, max(0.0, end - time.monotonic())))


def _source(ctx: HandlerContext) -> str:
    # Mock 产物必须带上上游 source，否则不同输入会产生相同输出，
    # 内容指纹会把它们误判为可复用结果（真实 pipeline 由像素内容天然区分）。
    return str(ctx.params.get("source", ""))


def ingest(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 0.5)
    return {"manifest.json": json.dumps({"source": ctx.config.get("source"), "frames": 24}).encode()}


def extract_frames(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 1.0)
    return {"frames/index.json": json.dumps(
        {"source": _source(ctx), "fps": ctx.config.get("target_fps", 12), "count": 24}
    ).encode()}


def segment_subject(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 1.2)
    return {"masks/index.json": json.dumps({"source": _source(ctx), "count": 24}).encode()}


def style_transfer(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 1.5)
    return {"styled/index.json": json.dumps(
        {"source": _source(ctx), "style": ctx.config.get("style"), "count": 24}
    ).encode()}


def upscale(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 1.0)
    return {"upscaled/index.json": json.dumps(
        {"source": _source(ctx), "scale": 2, "count": 24}
    ).encode()}


def compose_spritesheet(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 0.8)
    marker = _source(ctx).encode()
    return {"spritesheet.png": b"PNG-MOCK:" + marker, "preview.gif": b"GIF-MOCK:" + marker}


def publish(ctx: HandlerContext) -> dict[str, bytes]:
    _sleep(ctx, 0.5)
    marker = _source(ctx).encode()
    return {"bundle.json": json.dumps({"published": True, "source": _source(ctx)}).encode(),
            "checksums.txt": b"ok:" + marker}


HANDLERS = {
    "mock.ingest": ingest,
    "mock.extract_frames": extract_frames,
    "mock.segment_subject": segment_subject,
    "mock.style_transfer": style_transfer,
    "mock.upscale": upscale,
    "mock.compose_spritesheet": compose_spritesheet,
    "mock.publish": publish,
}
