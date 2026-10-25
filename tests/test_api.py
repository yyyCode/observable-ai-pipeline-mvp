import json
import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.db import query_all
from app.main import app


@pytest.fixture(scope="module")
def client():
    # 只启动一次执行器，避免 stop/start 导致后台线程与线程池复用问题。
    with TestClient(app) as c:
        yield c


def _wait(client, task_id, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = client.get(f"/api/tasks/{task_id}").json()
        if t["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
            return t
        time.sleep(0.2)
    raise AssertionError("timeout")


def _params(source, **extra):
    return {"source": source, "style": "x", "target_fps": 12, **extra}


def test_create_and_succeed(client):
    r = client.post("/api/tasks", json={
        "pipeline": "animated-asset", "version": "1",
        "params": _params(f"a-{uuid.uuid4().hex}.gif"),
    })
    assert r.status_code == 201, r.text
    task_id = r.json()["id"]
    t = _wait(client, task_id)
    assert t["status"] == "SUCCEEDED"
    assert len(t["steps"]) == 7

    # 提交时快照完整步骤定义，后续执行不再依赖可能变化的 Pipeline 文件。
    rows = query_all(
        "SELECT name, definition_json FROM steps WHERE task_id=?", (task_id,))
    definitions = {row["name"]: json.loads(row["definition_json"]) for row in rows}
    assert all(d.get("handler") for d in definitions.values())
    assert definitions["publish"]["handler"] == "mock.publish"
    assert definitions["publish"]["outputs"] == ["bundle.json", "checksums.txt"]


def test_retryable_failure_then_success(client):
    # source 必须每次唯一，否则上一次成功结果会让 fail_injections 因缓存命中而失效。
    r = client.post("/api/tasks", json={
        "pipeline": "animated-asset", "version": "1",
        "params": _params(f"b-{uuid.uuid4().hex}.gif",
                          fail_injections={"extract_frames": {"retryable_times": 1}}),
    })
    task_id = r.json()["id"]
    t = _wait(client, task_id)
    assert t["status"] == "SUCCEEDED"
    attempts = client.get(f"/api/tasks/{task_id}/steps/extract_frames/attempts").json()
    assert len(attempts) == 2


def test_manual_retry_uses_snapshot_without_pipeline_file(client):
    r = client.post("/api/tasks", json={
        "pipeline": "animated-asset", "version": "1",
        "params": _params(f"c-{uuid.uuid4().hex}.gif",
                          fail_injections={"segment_subject": {"fatal_attempt": 1}}),
    })
    assert r.status_code == 201, r.text
    task_id = r.json()["id"]
    failed = _wait(client, task_id)
    assert failed["status"] == "FAILED"
    statuses = {s["name"]: s["status"] for s in failed["steps"]}
    assert statuses["segment_subject"] == "FAILED"
    assert statuses["publish"] == "SKIPPED"

    # 重试不再读取 Pipeline 文件；若实现回退到磁盘，这里会直接失败。
    original = settings.pipeline_dir
    object.__setattr__(settings, "pipeline_dir",
                       Path(f"missing-pipeline-{uuid.uuid4().hex}"))
    try:
        rr = client.post(f"/api/tasks/{task_id}/steps/segment_subject/retry")
        assert rr.status_code == 200, rr.text
        t = _wait(client, task_id)
    finally:
        object.__setattr__(settings, "pipeline_dir", original)

    assert t["status"] == "SUCCEEDED"
    assert all(s["status"] == "SUCCEEDED" for s in t["steps"])
    assert all(not s["reused"] for s in t["steps"])
    segment = client.get(
        f"/api/tasks/{task_id}/steps/segment_subject/attempts").json()
    publish = client.get(f"/api/tasks/{task_id}/steps/publish/attempts").json()
    assert len(segment) == 2
    assert len(publish) == 1

    events = client.get(f"/api/tasks/{task_id}/events").json()
    retried = next(e for e in events if e["type"] == "task.retried")
    assert "publish" in retried["payload"]["steps"]
