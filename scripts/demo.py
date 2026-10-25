#!/usr/bin/env python3
"""One-command demo for the observable AI pipeline MVP.

Scenarios: normal, retry, fatal, cache, gpu, cancel, crash.
Run `python scripts/demo.py all` (default) or a single scenario name.
"""
import argparse
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
PORT = int(os.getenv("DEMO_PORT", "8020"))
BASE = f"http://127.0.0.1:{PORT}"
GPU_ENV = {"GPU_DEVICES": '[{"id":"gpu0","memory_mb":4096}]'}


def server_log_path(data_dir: Path) -> Path:
    return data_dir / "server.log"


def start_server(data_dir: Path, extra_env: dict | None = None) -> subprocess.Popen:
    log_path = server_log_path(data_dir)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["DATA_DIR"] = str(data_dir)
    env["LOG_DIR"] = str(data_dir / "logs")
    env.update(extra_env or {})
    cmd = [sys.executable, "-m", "uvicorn", "app.main:app",
           "--host", "127.0.0.1", "--port", str(PORT)]
    log_file = open(log_path, "ab")
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    for _ in range(200):
        if proc.poll() is not None:
            log_file.close()
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"server exited during startup:\n{tail}")
        try:
            if httpx.get(f"{BASE}/healthz", timeout=0.5).status_code == 200:
                return proc
        except Exception:
            pass
        time.sleep(0.1)
    proc.kill()
    log_file.close()
    raise RuntimeError("server failed to start within 20s")


def wait_task(client: httpx.Client, task_id: str, timeout: float = 120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = client.get(f"/api/tasks/{task_id}").json()
        if task["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
            return task
        time.sleep(0.2)
    raise TimeoutError(f"task {task_id} did not finish in {timeout}s")


def _src(tag: str) -> str:
    # 每个场景使用唯一 source，避免指纹命中上一场景的缓存而跳过故障注入。
    return f"demo/{tag}-{uuid.uuid4().hex[:8]}.gif"


def create(client: httpx.Client, params: dict | None = None) -> str:
    payload = {
        "pipeline": "animated-asset", "version": "1",
        "params": {"source": _src("task"), "style": "watercolor", "target_fps": 12,
                   **(params or {})},
    }
    response = client.post("/api/tasks", json=payload)
    response.raise_for_status()
    return response.json()["id"]


def wait_step(client: httpx.Client, task_id: str, step: str, status: str = "RUNNING",
              timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = client.get(f"/api/tasks/{task_id}").json()
        for item in task["steps"]:
            if item["name"] == step and item["status"] == status:
                return task
        time.sleep(0.1)
    raise AssertionError(f"{step} never reached {status}")


def scenario_normal(client: httpx.Client) -> str:
    task_id = create(client, {"source": _src("normal")})
    task = wait_task(client, task_id)
    assert task["status"] == "SUCCEEDED", task
    assert len(task["steps"]) == 7
    artifacts = client.get(f"/api/tasks/{task_id}/artifacts").json()
    assert len(artifacts) >= 9, artifacts
    return f"normal PASS ({len(artifacts)} artifacts)"


def scenario_retry(client: httpx.Client) -> str:
    task_id = create(client, {
        "source": _src("retry"),
        "fail_injections": {"extract_frames": {"retryable_times": 1}},
    })
    task = wait_task(client, task_id)
    assert task["status"] == "SUCCEEDED", task
    attempts = client.get(f"/api/tasks/{task_id}/steps/extract_frames/attempts").json()
    assert len(attempts) == 2, attempts
    assert attempts[0]["status"] == "FAILED" and attempts[1]["status"] == "SUCCEEDED", attempts
    return "retry PASS (transient failure, 2 attempts)"


def scenario_fatal(client: httpx.Client) -> str:
    task_id = create(client, {
        "source": _src("fatal"),
        "fail_injections": {"segment_subject": {"fatal_attempt": 1}},
    })
    task = wait_task(client, task_id)
    assert task["status"] == "FAILED", task
    statuses = {s["name"]: s["status"] for s in task["steps"]}
    assert statuses["segment_subject"] == "FAILED", statuses
    assert statuses["style_transfer"] == "SKIPPED", statuses
    return "fatal PASS (fatal failure, downstream skipped)"


def scenario_cache(client: httpx.Client) -> str:
    source = _src("cache")
    first = create(client, {"source": source})
    wait_task(client, first)
    second = create(client, {"source": source})
    task = wait_task(client, second)
    assert task["status"] == "SUCCEEDED", task
    reused = sum(1 for s in task["steps"] if s["reused"])
    assert reused == 7, task["steps"]
    return "cache PASS (7/7 steps reused across tasks)"


def scenario_gpu(client: httpx.Client) -> str:
    slow = {"slow_steps": {"segment_subject": 3, "style_transfer": 3}}
    task_a = create(client, {"source": _src("gpu-a"), **slow})
    task_b = create(client, {
        "source": _src("gpu-b"), **slow,
        "fail_injections": {"style_transfer": {"retryable_times": 1, "code": "CUDA_OOM"}},
    })
    gpu = {}
    for _ in range(250):
        gpu = client.get("/api/gpu").json()
        if gpu["queue"]:
            break
        time.sleep(0.1)
    assert gpu.get("queue"), gpu
    a = wait_task(client, task_a)
    b = wait_task(client, task_b)
    assert a["status"] == "SUCCEEDED" and b["status"] == "SUCCEEDED", (a, b)
    attempts = client.get(f"/api/tasks/{task_b}/steps/style_transfer/attempts").json()
    assert any(x["error_code"] == "CUDA_OOM" for x in attempts), attempts
    return "gpu PASS (queue observed, CUDA_OOM retried, both tasks succeeded)"


def scenario_cancel(client: httpx.Client) -> str:
    task_id = create(client, {"source": _src("cancel"), "slow_steps": {"style_transfer": 6}})
    wait_step(client, task_id, "style_transfer")
    client.post(f"/api/tasks/{task_id}/cancel").raise_for_status()
    task = wait_task(client, task_id, timeout=30)
    assert task["status"] == "CANCELED", task
    statuses = {s["name"]: s["status"] for s in task["steps"]}
    assert statuses["ingest"] == "SUCCEEDED", statuses
    assert statuses["style_transfer"] == "CANCELED", statuses
    artifacts = client.get(f"/api/tasks/{task_id}/artifacts").json()
    assert artifacts, "upstream artifacts should be preserved"
    return "cancel PASS (running step canceled, upstream artifacts preserved)"


def scenario_crash() -> str:
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp)
        proc = start_server(data, GPU_ENV)
        try:
            with httpx.Client(base_url=BASE, timeout=10) as client:
                task_id = create(client, {
                    "source": _src("crash"),
                    "slow_steps": {"style_transfer": 8},
                })
                wait_step(client, task_id, "style_transfer", timeout=40)
            proc.kill()
            proc.wait(timeout=10)
            proc = start_server(data, GPU_ENV)
            with httpx.Client(base_url=BASE, timeout=10) as client:
                task = wait_task(client, task_id, timeout=120)
                assert task["status"] == "SUCCEEDED", task
                events = client.get(f"/api/tasks/{task_id}/events").json()
                assert any(e["type"] == "recovery.resumed" for e in events), events
            return "crash PASS (hard kill, RUNNING step resumed, task succeeded)"
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)


SCENARIOS = {
    "normal": scenario_normal,
    "retry": scenario_retry,
    "fatal": scenario_fatal,
    "cache": scenario_cache,
    "gpu": scenario_gpu,
    "cancel": scenario_cancel,
}


def report(message: str) -> None:
    # Windows 控制台默认 GBK，避免对勾等 Unicode 字符触发 UnicodeEncodeError。
    print(f"[PASS] {message}", flush=True)


def run(names: list[str]) -> None:
    if names == ["crash"]:
        report(scenario_crash())
        return
    with tempfile.TemporaryDirectory() as tmp:
        proc = start_server(Path(tmp), GPU_ENV)
        try:
            with httpx.Client(base_url=BASE, timeout=15) as client:
                for name in names:
                    if name == "crash":
                        # crash 场景需要独占端口，稍后单独执行。
                        continue
                    report(SCENARIOS[name](client))
            if "crash" in names:
                proc.kill()
                proc.wait(timeout=10)
                report(scenario_crash())
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", nargs="?", default="all",
                        choices=["all", *SCENARIOS.keys(), "crash"])
    args = parser.parse_args()
    names = list(SCENARIOS.keys()) + ["crash"] if args.scenario == "all" else [args.scenario]
    run(names)


if __name__ == "__main__":
    main()
