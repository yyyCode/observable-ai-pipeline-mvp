import asyncio
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.cas import path_for
from app.config import settings
from app.db import query_all, query_one, tx
from app.domain import StepStatus, TaskStatus, assert_transition, iso, new_id
from app.events import append_event, list_events
from app.gpu import GPU
from app.pipeline import load_pipeline, resolve_effective_config, validate_gpu, validate_params

router = APIRouter(prefix="/api")


class CreateTask(BaseModel):
    pipeline: str
    version: str
    params: dict = Field(default_factory=dict)
    callback_url: str | None = None


@router.post("/tasks", status_code=201)
def create_task(body: CreateTask):
    if body.callback_url:
        parsed = urlparse(body.callback_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise HTTPException(status_code=422, detail="callback_url must be an absolute http(s) URL")
    pipeline = load_pipeline(settings.pipeline_dir, body.pipeline, body.version)
    validate_params(pipeline, body.params)
    validate_gpu(pipeline, settings.gpu_devices)
    task_id = new_id()
    trace_id = new_id()
    with tx() as conn:
        conn.execute(
            """INSERT INTO tasks(id,pipeline_name,pipeline_version,params_json,status,
                   callback_url,trace_id,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (task_id, body.pipeline, body.version, json.dumps(body.params), TaskStatus.PENDING,
             body.callback_url, trace_id, iso(), iso()),
        )
        for s in pipeline["steps"]:
            effective = resolve_effective_config(s, body.params)
            retry = s.get("retry") or {}
            conn.execute(
                """INSERT INTO steps(id,task_id,name,step_order,status,depends_on_json,config_json,
                       retry_max,retry_count,gpu_count,gpu_memory_mb,timeout_s,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (new_id(), task_id, s["name"], s["order"], StepStatus.PENDING,
                 json.dumps(s["depends_on"]), json.dumps(effective),
                 int(retry.get("max_attempts", 3)), 0,
                 int(s["gpu"]["count"]), int(s["gpu"]["memory_mb"]),
                 int(s.get("timeout_s", 180)), iso()),
            )
        append_event(conn, task_id, "task.created", payload=body.params, trace_id=trace_id)
    return {"id": task_id, "status": TaskStatus.PENDING,
            "links": {"self": f"/api/tasks/{task_id}", "events": f"/api/tasks/{task_id}/events"}}


def _loads(value, default=None):
    if value is None:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _duration_s(started_at: str | None, finished_at: str | None) -> float | None:
    if not started_at:
        return None
    start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    end = (datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
           if finished_at else datetime.now(timezone.utc))
    return round((end - start).total_seconds(), 1)


def _step_view(s: dict) -> dict:
    return {
        "id": s["id"], "name": s["name"], "order": s["step_order"], "status": s["status"],
        "depends_on": _loads(s["depends_on_json"], []),
        "config": _loads(s["config_json"], {}),
        "retry_max": s["retry_max"], "retry_count": s["retry_count"],
        "timeout_s": s["timeout_s"],
        "reused": bool(s["reused"]), "error_code": s["error_code"],
        "error_message": s["error_message"], "progress": s["progress"],
        "attempt_count": s.get("attempt_count", 0),
        "fingerprint": s["fingerprint"], "started_at": s["started_at"],
        "finished_at": s["finished_at"],
    }


@router.get("/tasks")
def list_tasks(status: str | None = None, limit: int = 50, cursor: str | None = None):
    limit = max(1, min(int(limit), 200))
    clauses, params = [], []
    if status:
        clauses.append("status=?")
        params.append(status)
    if cursor:
        clauses.append("created_at<?")
        params.append(cursor)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = query_all(
        f"SELECT * FROM tasks{where} ORDER BY created_at DESC LIMIT ?", (*params, limit)
    )
    out = []
    for t in rows:
        steps = query_all(
            "SELECT name, status, reused FROM steps WHERE task_id=? ORDER BY step_order",
            (t["id"],),
        )
        total = len(steps)
        succeeded = sum(1 for s in steps if s["status"] == "SUCCEEDED")
        current = next((s["name"] for s in steps if s["status"] in
                        ("RUNNING", "RETRYING", "WAITING_RESOURCE")), None)
        if current is None:
            current = next((s["name"] for s in steps if s["status"] in
                            ("READY", "PENDING")), None)
        out.append({
            "id": t["id"], "status": t["status"],
            "progress": round(succeeded * 100 / total) if total else 0,
            "current_step": current,
            "duration_s": _duration_s(t["started_at"], t["finished_at"]),
            "reused_steps": sum(1 for s in steps if s["reused"]),
            "created_at": t["created_at"],
        })
    return out


@router.get("/tasks/{task_id}")
def get_task(task_id: str):
    t = _task_or_404(task_id)
    steps = query_all(
        """SELECT s.*, (SELECT COUNT(*) FROM step_attempts a WHERE a.step_id=s.id) AS attempt_count
           FROM steps s WHERE s.task_id=? ORDER BY s.step_order""", (task_id,))
    total = len(steps)
    succeeded = sum(1 for s in steps if s["status"] == "SUCCEEDED")
    artifacts = query_all(
        """SELECT a.*, s.name AS step FROM artifacts a JOIN steps s ON s.id=a.step_id
           WHERE a.task_id=? ORDER BY s.step_order, a.name""", (task_id,))
    return {
        "id": t["id"], "pipeline": t["pipeline_name"], "version": t["pipeline_version"],
        "status": t["status"], "params": _loads(t["params_json"], {}),
        "created_at": t["created_at"], "started_at": t["started_at"],
        "finished_at": t["finished_at"],
        "duration_s": _duration_s(t["started_at"], t["finished_at"]),
        "progress": round(succeeded * 100 / total) if total else 0,
        "error_code": t["error_code"], "error_message": t["error_message"],
        "cancel_requested": bool(t["cancel_requested"]),
        "steps": [_step_view(s) for s in steps],
        "events": list_events(task_id, 0, 500),
        "artifacts": [{
            "id": a["id"], "step": a["step"], "name": a["name"],
            "sha256": a["sha256"], "size_bytes": a["size_bytes"],
            "download_url": f"/api/artifacts/{a['id']}/download",
        } for a in artifacts],
    }


@router.get("/tasks/{task_id}/steps")
def get_steps(task_id: str):
    _task_or_404(task_id)
    rows = query_all(
        """SELECT s.*, (SELECT COUNT(*) FROM step_attempts a WHERE a.step_id=s.id) AS attempt_count
           FROM steps s WHERE s.task_id=? ORDER BY s.step_order""", (task_id,))
    return [_step_view(s) for s in rows]


@router.get("/tasks/{task_id}/steps/{name}/attempts")
def get_attempts(task_id: str, name: str):
    _task_or_404(task_id)
    step = query_one("SELECT id FROM steps WHERE task_id=? AND name=?", (task_id, name))
    if not step:
        raise HTTPException(404, detail="step not found")
    rows = query_all(
        "SELECT * FROM step_attempts WHERE step_id=? ORDER BY attempt_no", (step["id"],))
    for r in rows:
        r["metrics"] = _loads(r.pop("metrics_json", None), {})
    return rows


@router.get("/tasks/{task_id}/events")
def get_events(task_id: str, after: int = 0, limit: int = 200):
    _task_or_404(task_id)
    return list_events(task_id, int(after), min(max(int(limit), 1), 1000))


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    t = _task_or_404(task_id)
    if t["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
        raise HTTPException(409, detail="task already terminal")
    with tx() as conn:
        row = conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
            raise HTTPException(409, detail="task already terminal")
        conn.execute("UPDATE tasks SET cancel_requested=1, updated_at=? WHERE id=?",
                     (iso(), task_id))
        append_event(conn, task_id, "task.cancel_requested", trace_id=t["trace_id"])
    return {"id": task_id, "status": t["status"], "cancel_requested": True}


@router.get("/tasks/{task_id}/artifacts")
def get_artifacts(task_id: str):
    _task_or_404(task_id)
    rows = query_all(
        """SELECT a.*, s.name AS step FROM artifacts a JOIN steps s ON s.id=a.step_id
           WHERE a.task_id=? ORDER BY s.step_order, a.name""", (task_id,))
    for r in rows:
        r["download_url"] = f"/api/artifacts/{r['id']}/download"
    return rows


@router.get("/artifacts/{artifact_id}")
def get_artifact(artifact_id: str):
    row = query_one("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    if not row:
        raise HTTPException(404, detail="artifact not found")
    row["download_url"] = f"/api/artifacts/{artifact_id}/download"
    return row


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(artifact_id: str):
    row = query_one("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    if not row:
        raise HTTPException(404, detail="artifact not found")
    p = path_for(row["sha256"])
    if not p.exists():
        raise HTTPException(410, detail="artifact content missing")
    return FileResponse(p, media_type=row["mime"], filename=row["name"])


@router.get("/gpu")
def gpu_status():
    return GPU.snapshot()


@router.get("/overview")
def overview():
    counts = {r["status"]: int(r["n"]) for r in
              query_all("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")}
    total = sum(counts.values())
    succeeded = counts.get("SUCCEEDED", 0)
    avg = query_one(
        """SELECT AVG((julianday(finished_at)-julianday(started_at))*86400) AS s
           FROM tasks WHERE status='SUCCEEDED' AND started_at IS NOT NULL
             AND finished_at IS NOT NULL"""
    )["s"]
    cache_hits = query_one("SELECT COALESCE(SUM(hit_count),0) AS n FROM step_cache")["n"]
    return {
        "tasks_total": total,
        "tasks_active": counts.get("PENDING", 0) + counts.get("RUNNING", 0),
        "tasks_succeeded": succeeded,
        "tasks_failed": counts.get("FAILED", 0),
        "success_rate_pct": round(succeeded * 100 / total, 1) if total else 0,
        "avg_duration_s": round(float(avg), 2) if avg is not None else 0,
        "cache_hits": int(cache_hits),
    }


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: str):
    task = _task_or_404(task_id)
    if task["status"] != TaskStatus.FAILED:
        raise HTTPException(409, detail="only FAILED tasks are retryable")
    pipeline = load_pipeline(settings.pipeline_dir, task["pipeline_name"], task["pipeline_version"])
    with tx() as conn:
        failed = conn.execute(
            "SELECT name FROM steps WHERE task_id=? AND status='FAILED' ORDER BY step_order",
            (task_id,),
        ).fetchall()
        if failed:
            # 多个独立分支可能同时失败；逐一重置，重复的重置是幂等的。
            for row in failed:
                _reset_downstream(conn, task_id, pipeline, row["name"])
        else:
            conn.execute("UPDATE steps SET status='PENDING', retry_count=0, updated_at=? WHERE task_id=? AND status IN ('CANCELED','SKIPPED')",
                         (iso(), task_id))
        conn.execute(
            "UPDATE tasks SET status='RUNNING', cancel_requested=0, finished_at=NULL, error_code=NULL, error_message=NULL, updated_at=? WHERE id=?",
            (iso(), task_id),
        )
        append_event(conn, task_id, "task.retried")
    return {"id": task_id, "status": "RUNNING"}


def _reset_downstream(conn, task_id: str, pipeline: dict, step_name: str) -> None:
    from app.pipeline import dependents
    targets = {step_name} | dependents(pipeline, step_name)
    for name in targets:
        current = conn.execute(
            "SELECT status FROM steps WHERE task_id=? AND name=?", (task_id, name)
        ).fetchone()
        if current and current["status"] != "PENDING":
            assert_transition("step", current["status"], StepStatus.PENDING)
        conn.execute(
            """UPDATE steps SET status='PENDING', retry_count=0, error_class=NULL, error_code=NULL,
                   error_message=NULL, next_retry_at=NULL, started_at=NULL, finished_at=NULL,
                   fingerprint=NULL, reused=0, progress=0, updated_at=?
               WHERE task_id=? AND name=?""",
            (iso(), task_id, name),
        )
        conn.execute(
            "DELETE FROM artifacts WHERE task_id=? AND step_id=(SELECT id FROM steps WHERE task_id=? AND name=?)",
            (task_id, task_id, name),
        )


def _task_or_404(task_id: str) -> dict:
    task = query_one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if not task:
        raise HTTPException(404, detail="task not found")
    return task


@router.post("/tasks/{task_id}/steps/{name}/retry")
def retry_step(task_id: str, name: str):
    task = _task_or_404(task_id)
    if task["status"] not in ("RUNNING", "FAILED"):
        raise HTTPException(409, detail="task is not retryable")
    if task["cancel_requested"]:
        raise HTTPException(409, detail="task is canceling")
    step = query_one("SELECT * FROM steps WHERE task_id=? AND name=?", (task_id, name))
    if not step or step["status"] != "FAILED":
        raise HTTPException(409, detail="only FAILED steps are retryable")
    pipeline = load_pipeline(settings.pipeline_dir, task["pipeline_name"], task["pipeline_version"])
    with tx() as conn:
        _reset_downstream(conn, task_id, pipeline, name)
        conn.execute(
            """UPDATE tasks SET status='RUNNING', finished_at=NULL, error_code=NULL,
                   error_message=NULL, updated_at=? WHERE id=?""",
            (iso(), task_id),
        )
        append_event(conn, task_id, "task.retried", payload={"step": name})
    return {"id": task_id, "status": "RUNNING"}


@router.get("/tasks/{task_id}/stream")
async def stream(task_id: str, request: Request, after: int = 0):
    _task_or_404(task_id)

    async def gen():
        try:
            last = int(after or request.headers.get("last-event-id", "0") or 0)
        except ValueError:
            last = 0
        idle = 0
        while True:
            if await request.is_disconnected():
                break
            events = await asyncio.to_thread(list_events, task_id, last, 100)
            if events:
                idle = 0
                for e in events:
                    last = e["id"]
                    yield f'id: {e["id"]}\nevent: {e["type"]}\ndata: {json.dumps(e, ensure_ascii=False)}\n\n'
                await asyncio.sleep(0.2)
            else:
                idle += 1
                if idle % 15 == 0:
                    yield ": keepalive\n\n"
                task = await asyncio.to_thread(query_one, "SELECT status FROM tasks WHERE id=?", (task_id,))
                if task and task["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
                    break
                await asyncio.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")
