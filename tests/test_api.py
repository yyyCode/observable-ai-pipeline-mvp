import time
import uuid

import pytest
from fastapi.testclient import TestClient

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
    t = _wait(client, r.json()["id"])
    assert t["status"] == "SUCCEEDED"
    assert len(t["steps"]) == 7


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
