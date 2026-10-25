import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath

from app import metrics
from app.cas import exists_and_valid, put_bytes
from app.config import settings
from app.db import query_all, query_one, tx
from app.domain import (
    StepStatus, TaskStatus, active_step_statuses, assert_transition,
    backoff_ms, iso, iso_plus, new_id,
)
from app.errors import CanceledError, FatalError, PipelineError, RetryableError
from app.events import append_event
from app.fingerprint import step_fingerprint
from app.gpu import GPU
from app.handlers import HANDLER_VERSION, HANDLERS, HandlerContext
from app.pipeline import load_pipeline, resolve_effective_config

log = logging.getLogger(__name__)
_stop = threading.Event()
_pool: ThreadPoolExecutor | None = None
_cancel_events: dict[str, threading.Event] = {}
_cancel_reasons: dict[str, str] = {}
_cancel_lock = threading.Lock()


def start() -> None:
    global _pool
    _stop.clear()
    _pool = ThreadPoolExecutor(max_workers=settings.max_concurrency)
    _reconcile()
    threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler").start()
    threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat").start()
    metrics.EXECUTOR_UP.set(1)


def stop() -> None:
    _stop.set()
    if _pool is not None:
        _pool.shutdown(wait=False, cancel_futures=False)
    metrics.EXECUTOR_UP.set(0)


def _scheduler_loop() -> None:
    while not _stop.is_set():
        try:
            _tick()
        except Exception:
            log.exception("scheduler tick failed")
        _stop.wait(0.5)


def _tick() -> None:
    _promote_tasks()
    _handle_cancels()
    _advance_pending()
    _promote_retry()
    _finalize_tasks()
    _dispatch_ready()


def _promote_tasks() -> None:
    tasks = query_all("SELECT * FROM tasks WHERE status='PENDING' AND cancel_requested=0")
    for t in tasks:
        with tx() as conn:
            row = conn.execute("SELECT status FROM tasks WHERE id=?", (t["id"],)).fetchone()
            if row["status"] != "PENDING":
                continue
            assert_transition("task", row["status"], TaskStatus.RUNNING)
            conn.execute(
                "UPDATE tasks SET status='RUNNING', started_at=COALESCE(started_at,?), updated_at=? WHERE id=?",
                (iso(), iso(), t["id"]),
            )
            append_event(conn, t["id"], "task.started", trace_id=t["trace_id"])


def _advance_pending() -> None:
    rows = query_all(
        """SELECT s.*, t.pipeline_name, t.pipeline_version, t.params_json, t.trace_id
           FROM steps s JOIN tasks t ON t.id=s.task_id
           WHERE t.status='RUNNING' AND s.status='PENDING' ORDER BY t.created_at, s.step_order"""
    )
    for s in rows:
        deps = json.loads(s["depends_on_json"])
        if not deps:
            _transition_step(s, StepStatus.READY, "step.ready")
            continue
        dep_rows = query_all(
            f"SELECT name, status FROM steps WHERE task_id=? AND name IN ({','.join('?' * len(deps))})",
            (s["task_id"], *deps),
        )
        statuses = {r["name"]: r["status"] for r in dep_rows}
        if all(statuses.get(d) == StepStatus.SUCCEEDED for d in deps):
            _transition_step(s, StepStatus.READY, "step.ready")
        elif any(statuses.get(d) in {StepStatus.FAILED, StepStatus.SKIPPED, StepStatus.CANCELED} for d in deps):
            _transition_step(s, StepStatus.SKIPPED, "step.skipped")


def _transition_step(step: dict, to: str, event: str, **payload) -> None:
    with tx() as conn:
        row = conn.execute("SELECT status FROM steps WHERE id=?", (step["id"],)).fetchone()
        assert_transition("step", row["status"], to)
        conn.execute("UPDATE steps SET status=?, updated_at=? WHERE id=?", (to, iso(), step["id"]))
        append_event(conn, step["task_id"], event, step_id=step["id"], payload=payload,
                     trace_id=step.get("trace_id"))
    metrics.STEPS_TOTAL.labels(
        status=to.value if hasattr(to, "value") else to, step=step["name"]
    ).inc()


def _promote_retry() -> None:
    rows = query_all(
        """SELECT s.*, t.trace_id FROM steps s JOIN tasks t ON t.id=s.task_id
           WHERE s.status='RETRYING' AND s.next_retry_at IS NOT NULL AND s.next_retry_at<=?""",
        (iso(),),
    )
    for s in rows:
        _transition_step(s, StepStatus.READY, "step.ready", reason="retry_due")


def _finalize_tasks() -> None:
    tasks = query_all("SELECT * FROM tasks WHERE status='RUNNING'")
    for t in tasks:
        steps = query_all("SELECT * FROM steps WHERE task_id=?", (t["id"],))
        statuses = [s["status"] for s in steps]
        if all(s == StepStatus.SUCCEEDED for s in statuses):
            _finish_task(t, TaskStatus.SUCCEEDED, "task.succeeded")
        elif t["cancel_requested"] and not any(s in active_step_statuses() for s in statuses):
            _finish_task(t, TaskStatus.CANCELED, "task.canceled")
        elif any(s == StepStatus.FAILED for s in statuses) and not any(
            s in {StepStatus.PENDING, StepStatus.READY, StepStatus.RUNNING,
                  StepStatus.RETRYING, StepStatus.WAITING_RESOURCE} for s in statuses
        ):
            failed = next(s for s in steps if s["status"] == StepStatus.FAILED)
            _finish_task(t, TaskStatus.FAILED, "task.failed",
                         error_code=failed["error_code"], error_message=failed["error_message"])


def _finish_task(task: dict, status: str, event: str, **payload) -> None:
    with tx() as conn:
        assert_transition("task", task["status"], status)
        conn.execute(
            """UPDATE tasks SET status=?, finished_at=?, updated_at=?,
                   error_code=?, error_message=? WHERE id=?""",
            (status, iso(), iso(), payload.get("error_code"), payload.get("error_message"), task["id"]),
        )
        append_event(conn, task["id"], event, payload=payload, trace_id=task["trace_id"])
        # outbox 与任务终态在同一事务提交，避免状态已改但回调丢失。
        from app import callback
        callback.enqueue_terminal({**task, "status": status}, conn=conn)
    metrics.TASKS_TOTAL.labels(status=status.value if hasattr(status, "value") else status).inc()


def _dispatch_ready() -> None:
    running = query_one("SELECT COUNT(*) AS n FROM step_attempts WHERE status='RUNNING'")["n"]
    slots = settings.max_concurrency - int(running)
    if slots <= 0:
        return
    # 同时处理 READY 和 WAITING_RESOURCE；GPU 释放后在同一 tick 内唤醒排队步骤。
    rows = query_all(
        """SELECT s.*, t.pipeline_name, t.pipeline_version, t.params_json, t.trace_id,
                  t.status AS task_status, t.cancel_requested
           FROM steps s JOIN tasks t ON t.id=s.task_id
           WHERE s.status IN ('READY','WAITING_RESOURCE') AND t.status='RUNNING'
           ORDER BY t.created_at, s.step_order"""
    )
    for s in rows:
        if slots <= 0:
            break
        if s["cancel_requested"]:
            _transition_step(s, StepStatus.CANCELED, "step.canceled")
            continue
        step_def = _step_definition(s)
        prepared = _prepare(s, step_def, s["pipeline_name"], str(s["pipeline_version"]),
                            json.loads(s["params_json"]))
        if _try_reuse(s, prepared):
            continue

        attempt_id = new_id()
        device_ids = None
        if int(s["gpu_count"]) > 0:
            try:
                device_ids = GPU.acquire(attempt_id, int(s["gpu_count"]), int(s["gpu_memory_mb"]))
            except RetryableError as exc:
                # 超过设备容量的配置错误应在创建任务时拦截；这里保留 READY 下个 tick 再试。
                log.warning("gpu acquire rejected step=%s code=%s", s["name"], exc.code)
                continue
            if device_ids is None:
                if s["status"] != StepStatus.WAITING_RESOURCE:
                    _transition_step(s, StepStatus.WAITING_RESOURCE, "gpu.queued",
                                     memory_mb=s["gpu_memory_mb"])
                continue
            if s["status"] == StepStatus.WAITING_RESOURCE:
                _transition_step(s, StepStatus.READY, "step.ready", reason="gpu_available")

        try:
            with tx() as conn:
                current = conn.execute("SELECT status FROM steps WHERE id=?", (s["id"],)).fetchone()
                assert_transition("step", current["status"], StepStatus.RUNNING)
                conn.execute(
                    "UPDATE steps SET status=?, started_at=COALESCE(started_at,?), progress=10, updated_at=? WHERE id=?",
                    (StepStatus.RUNNING, iso(), iso(), s["id"]),
                )
                conn.execute(
                    """INSERT INTO step_attempts(id, step_id, attempt_no, status, worker_id,
                           lease_expires_at, started_at)
                       SELECT ?, ?, COALESCE(MAX(attempt_no),0)+1, 'RUNNING', ?, ?, ?
                       FROM step_attempts WHERE step_id=?""",
                    (attempt_id, s["id"], "worker-1", iso_plus(settings.lease_seconds), iso(), s["id"]),
                )
                append_event(conn, s["task_id"], "step.attempt.started", step_id=s["id"],
                             attempt_id=attempt_id, payload={"step": s["name"], "devices": device_ids},
                             trace_id=s["trace_id"])
                conn.execute(
                    "UPDATE tasks SET started_at=COALESCE(started_at,?), updated_at=? WHERE id=?",
                    (iso(), iso(), s["task_id"]),
                )
        except Exception:
            if device_ids:
                GPU.release(attempt_id, "dispatch_failed")
            raise

        try:
            _pool.submit(_run_attempt, s, attempt_id, prepared, step_def, device_ids)
        except Exception:
            if device_ids:
                GPU.release(attempt_id, "submit_failed")
            raise
        slots -= 1


def _step_definition(step: dict) -> dict:
    """优先使用任务提交时的定义快照；旧任务回退到磁盘 Pipeline。"""
    try:
        definition = json.loads(step.get("definition_json") or "{}")
    except (TypeError, ValueError):
        definition = {}
    if definition.get("handler"):
        return definition
    pipeline = load_pipeline(settings.pipeline_dir, step["pipeline_name"], step["pipeline_version"])
    return next(x for x in pipeline["steps"] if x["name"] == step["name"])


def _prepare(step: dict, step_def: dict, pipeline_name: str, pipeline_version: str,
             task_params: dict) -> dict:
    effective = resolve_effective_config(step_def, task_params)
    deps = json.loads(step["depends_on_json"])
    input_shas = []
    if deps:
        rows = query_all(
            f"""SELECT a.sha256 FROM artifacts a JOIN steps s ON s.id=a.step_id
                WHERE s.task_id=? AND s.name IN ({','.join('?' * len(deps))})
                ORDER BY s.name, a.name""",
            (step["task_id"], *deps),
        )
        input_shas = [r["sha256"] for r in rows]
    fp = step_fingerprint(pipeline_name, pipeline_version, step["name"],
                          effective, input_shas, HANDLER_VERSION)
    return {"fingerprint": fp, "effective": effective, "input_shas": input_shas,
            "pipeline_name": pipeline_name}


def _try_reuse(step: dict, prepared: dict) -> bool:
    row = query_one("SELECT * FROM step_cache WHERE fingerprint=?", (prepared["fingerprint"],))
    if not row:
        return False
    try:
        items = json.loads(row["artifacts_json"])
        valid = all(exists_and_valid(i["sha256"], i["size"]) for i in items)
    except (TypeError, ValueError, KeyError):
        valid = False
        items = []
    if not valid or not items:
        with tx() as conn:
            conn.execute("DELETE FROM step_cache WHERE fingerprint=?", (prepared["fingerprint"],))
        return False
    with tx() as conn:
        current = conn.execute("SELECT status FROM steps WHERE id=?", (step["id"],)).fetchone()
        current_status = current["status"]
        # 复用仍然走 READY/WAITING_RESOURCE -> RUNNING -> SUCCEEDED 的合法迁移，
        # 只是不执行 handler、不占 GPU；排队中的步骤先回到 READY。
        if current_status == "WAITING_RESOURCE":
            assert_transition("step", current_status, StepStatus.READY)
            conn.execute("UPDATE steps SET status='READY', updated_at=? WHERE id=?",
                         (iso(), step["id"]))
            current_status = "READY"
        assert_transition("step", current_status, StepStatus.RUNNING)
        conn.execute(
            "UPDATE steps SET status='RUNNING', started_at=COALESCE(started_at,?), updated_at=? WHERE id=?",
            (iso(), iso(), step["id"]),
        )
        attempt_id = new_id()
        conn.execute(
            """INSERT INTO step_attempts(id, step_id, attempt_no, status, started_at,
                   finished_at, metrics_json)
               VALUES (?, ?, (SELECT COALESCE(MAX(attempt_no),0)+1 FROM step_attempts WHERE step_id=?), 'SUCCEEDED', ?, ?, ?)""",
            (attempt_id, step["id"], step["id"], iso(), iso(), json.dumps({"reused": True})),
        )
        for item in items:
            conn.execute(
                """INSERT INTO artifacts(id, task_id, step_id, name, sha256, size_bytes, mime, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (new_id(), step["task_id"], step["id"], item["name"], item["sha256"],
                 item["size"], item.get("mime", "application/octet-stream"), iso()),
            )
        assert_transition("step", StepStatus.RUNNING, StepStatus.SUCCEEDED)
        conn.execute(
            """UPDATE steps SET status='SUCCEEDED', reused=1, progress=100, fingerprint=?,
                   error_class=NULL, error_code=NULL, error_message=NULL, next_retry_at=NULL,
                   finished_at=?, updated_at=? WHERE id=?""",
            (prepared["fingerprint"], iso(), iso(), step["id"]),
        )
        conn.execute(
            "UPDATE step_cache SET hit_count=hit_count+1, last_used_at=? WHERE fingerprint=?",
            (iso(), prepared["fingerprint"]),
        )
        append_event(conn, step["task_id"], "step.cache_hit", step_id=step["id"],
                     attempt_id=attempt_id, payload={"step": step["name"]}, trace_id=step.get("trace_id"))
        append_event(conn, step["task_id"], "step.succeeded", step_id=step["id"],
                     attempt_id=attempt_id, payload={"step": step["name"], "reused": True},
                     trace_id=step.get("trace_id"))
    metrics.CACHE_HITS.labels(step=step["name"]).inc()
    metrics.STEPS_TOTAL.labels(status="SUCCEEDED", step=step["name"]).inc()
    return True


def _run_attempt(step: dict, attempt_id: str, prepared: dict, step_def: dict, device_ids) -> None:
    cancel = threading.Event()
    retry_cfg = step_def.get("retry") or {}
    with _cancel_lock:
        _cancel_events[attempt_id] = cancel
    log_path = settings.log_dir / step["task_id"] / step["name"] / f"attempt-{attempt_id[:8]}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with tx() as conn:
        conn.execute("UPDATE step_attempts SET log_path=? WHERE id=?", (str(log_path), attempt_id))
    started = time.monotonic()
    try:
        attempt_no = _attempt_no(attempt_id)
        _inject_faults(step["name"], json.loads(step["params_json"]), attempt_no)
        ctx = HandlerContext(
            step_name=step["name"], config=prepared["effective"],
            params=json.loads(step["params_json"]),
            attempt_no=attempt_no, cancel_event=cancel,
            log=lambda rec: _append_log(log_path, rec),
        )
        outputs = HANDLERS[step_def["handler"]](ctx)
        if cancel.is_set():
            raise CanceledError("CANCELED", "canceled after handler")
        validate_outputs(step_def, outputs)
        _complete_success(step, attempt_id, outputs, prepared, time.monotonic() - started)
    except CanceledError as exc:
        reason = _cancel_reasons.get(attempt_id, "user")
        if reason == "timeout":
            exc = RetryableError("STEP_TIMEOUT", "step timed out")
            _finish_attempt_failure(step, attempt_id, exc, retryable=True, retry_cfg=retry_cfg)
        else:
            _finish_canceled(step, attempt_id)
    except RetryableError as exc:
        _finish_attempt_failure(step, attempt_id, exc, retryable=True, retry_cfg=retry_cfg)
    except FatalError as exc:
        _finish_attempt_failure(step, attempt_id, exc, retryable=False, retry_cfg=retry_cfg)
    except Exception as exc:
        log.exception("unexpected step error")
        _finish_attempt_failure(step, attempt_id,
                                RetryableError("INTERNAL_ERROR", str(exc)),
                                retryable=True, retry_cfg=retry_cfg)
    finally:
        GPU.release(attempt_id, "finished")
        with _cancel_lock:
            _cancel_events.pop(attempt_id, None)
            _cancel_reasons.pop(attempt_id, None)


def _attempt_no(attempt_id: str) -> int:
    row = query_one("SELECT attempt_no FROM step_attempts WHERE id=?", (attempt_id,))
    return int(row["attempt_no"]) if row else 1


def _inject_faults(step_name: str, params: dict, attempt_no: int) -> None:
    inj = (params.get("fail_injections") or {}).get(step_name) or {}
    if attempt_no <= int(inj.get("retryable_times", 0)):
        raise RetryableError(inj.get("code", "INJECTED_TRANSIENT"), "injected transient failure")
    if int(inj.get("fatal_attempt", 0)) == attempt_no:
        raise FatalError(inj.get("code", "INJECTED_FATAL"), "injected fatal failure")


def validate_outputs(step_def: dict, outputs) -> None:
    """在写入 CAS/数据库前校验 Handler 是否遵守步骤的输出契约。"""
    if not isinstance(outputs, dict):
        raise FatalError("OUTPUT_CONTRACT", "handler must return dict[str, bytes]")
    expected = set(step_def.get("outputs") or [])
    actual = set(outputs)
    missing, extra = sorted(expected - actual), sorted(actual - expected)
    if missing or extra:
        raise FatalError(
            "OUTPUT_CONTRACT",
            f"handler outputs do not match pipeline: missing={missing} extra={extra}",
        )
    for name, data in outputs.items():
        path = PurePosixPath(name)
        if not name or "\\" in name or path.is_absolute() or ".." in path.parts:
            raise FatalError("OUTPUT_CONTRACT", f"invalid output name: {name!r}")
        if not isinstance(data, (bytes, bytearray)):
            raise FatalError("OUTPUT_CONTRACT", f"output {name!r} is not bytes")


def _complete_success(step: dict, attempt_id: str, outputs: dict[str, bytes],
                      prepared: dict, elapsed: float) -> None:
    items = []
    for name, data in outputs.items():
        sha, size = put_bytes(data)
        items.append({"name": name, "sha256": sha, "size": size, "mime": "application/octet-stream"})
    with tx() as conn:
        current = conn.execute("SELECT status FROM steps WHERE id=?", (step["id"],)).fetchone()
        assert_transition("step", current["status"], StepStatus.SUCCEEDED)
        for item in items:
            conn.execute(
                """INSERT INTO artifacts(id, task_id, step_id, name, sha256, size_bytes, mime, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (new_id(), step["task_id"], step["id"], item["name"], item["sha256"],
                 item["size"], item["mime"], iso()),
            )
        conn.execute(
            """INSERT INTO step_cache(fingerprint, artifacts_json, pipeline_name, step_name,
                   hit_count, created_at, last_used_at)
               VALUES (?, ?, ?, ?, 0, ?, ?)
               ON CONFLICT(fingerprint) DO UPDATE SET artifacts_json=excluded.artifacts_json,
                   last_used_at=excluded.last_used_at""",
            (prepared["fingerprint"], json.dumps(items), prepared["pipeline_name"],
             step["name"], iso(), iso()),
        )
        conn.execute(
            """UPDATE steps SET status='SUCCEEDED', fingerprint=?, progress=100,
                   error_class=NULL, error_code=NULL, error_message=NULL, next_retry_at=NULL,
                   finished_at=?, updated_at=? WHERE id=?""",
            (prepared["fingerprint"], iso(), iso(), step["id"]),
        )
        conn.execute(
            "UPDATE step_attempts SET status='SUCCEEDED', finished_at=?, metrics_json=? WHERE id=?",
            (iso(), json.dumps({"duration_s": round(elapsed, 3), "reused": False}), attempt_id),
        )
        append_event(conn, step["task_id"], "step.succeeded", step_id=step["id"],
                     attempt_id=attempt_id, payload={"step": step["name"]}, trace_id=step.get("trace_id"))
    metrics.STEPS_TOTAL.labels(status="SUCCEEDED", step=step["name"]).inc()
    metrics.STEP_DURATION.labels(step=step["name"], outcome="success").observe(elapsed)
    metrics.STEP_ATTEMPTS.labels(step=step["name"], error_class="none").inc()


def _finish_attempt_failure(step: dict, attempt_id: str, exc: PipelineError,
                            retryable: bool, retry_cfg: dict | None = None) -> None:
    now_iso = iso()
    with tx() as conn:
        step_row = conn.execute("SELECT * FROM steps WHERE id=?", (step["id"],)).fetchone()
        # 只处理 RUNNING 步骤的失败/取消迁移，防止取消/删除后的预执行失败造成非法状态。
        if step_row["status"] != "RUNNING":
            return
        attempt = conn.execute("SELECT * FROM step_attempts WHERE id=?", (attempt_id,)).fetchone()
        if attempt is None:
            # 防御性补记：正常路径不会出现，但不能让失败没有 attempt 记录。
            attempt_no = conn.execute(
                "SELECT COALESCE(MAX(attempt_no),0)+1 AS n FROM step_attempts WHERE step_id=?",
                (step["id"],),
            ).fetchone()["n"]
            conn.execute(
                """INSERT INTO step_attempts(id, step_id, attempt_no, status, error_class,
                       error_code, error_message, started_at, finished_at)
                   VALUES (?, ?, ?, 'FAILED', ?, ?, ?, ?, ?)""",
                (attempt_id, step["id"], attempt_no,
                 "RETRYABLE" if retryable else "FATAL", exc.code, exc.message, now_iso, now_iso),
            )
        elif attempt["status"] == "RUNNING":
            conn.execute(
                "UPDATE step_attempts SET status='FAILED', error_class=?, error_code=?, error_message=?, finished_at=? WHERE id=?",
                ("RETRYABLE" if retryable else "FATAL", exc.code, exc.message, now_iso, attempt_id),
            )
        retry_count = int(step_row["retry_count"])
        can_retry = retryable and retry_count + 1 < int(step_row["retry_max"])
        if can_retry:
            assert_transition("step", step_row["status"], StepStatus.RETRYING)
            base = int((retry_cfg or {}).get("backoff_base_ms", 1000))
            cap = int((retry_cfg or {}).get("backoff_max_ms", 10000))
            delay = backoff_ms(base, cap, retry_count)
            conn.execute(
                """UPDATE steps SET status='RETRYING', retry_count=retry_count+1,
                       next_retry_at=?, error_class=?, error_code=?, error_message=?, updated_at=?
                   WHERE id=?""",
                (iso_plus(delay / 1000), "RETRYABLE", exc.code, exc.message, now_iso, step["id"]),
            )
            append_event(conn, step["task_id"], "step.retrying", step_id=step["id"],
                         attempt_id=attempt_id,
                         payload={"step": step["name"], "code": exc.code, "retry_in_ms": delay},
                         trace_id=step.get("trace_id"))
        else:
            assert_transition("step", step_row["status"], StepStatus.FAILED)
            conn.execute(
                """UPDATE steps SET status='FAILED', error_class=?, error_code=?, error_message=?,
                       next_retry_at=NULL, finished_at=?, updated_at=? WHERE id=?""",
                ("RETRYABLE" if retryable else "FATAL", exc.code, exc.message, now_iso, now_iso, step["id"]),
            )
            append_event(conn, step["task_id"], "step.failed", step_id=step["id"],
                         attempt_id=attempt_id,
                         payload={"step": step["name"], "code": exc.code}, trace_id=step.get("trace_id"))
    metrics.STEP_ATTEMPTS.labels(step=step["name"], error_class="RETRYABLE" if retryable else "FATAL").inc()


def _finish_canceled(step: dict, attempt_id: str) -> None:
    with tx() as conn:
        current = conn.execute("SELECT status FROM steps WHERE id=?", (step["id"],)).fetchone()
        if current["status"] != "RUNNING":
            return
        assert_transition("step", current["status"], StepStatus.CANCELED)
        conn.execute("UPDATE step_attempts SET status='CANCELED', finished_at=? WHERE id=? AND status='RUNNING'",
                     (iso(), attempt_id))
        conn.execute("UPDATE steps SET status='CANCELED', finished_at=?, updated_at=? WHERE id=?",
                     (iso(), iso(), step["id"]))
        append_event(conn, step["task_id"], "step.canceled", step_id=step["id"], attempt_id=attempt_id,
                     payload={"step": step["name"]}, trace_id=step.get("trace_id"))


def _heartbeat_loop() -> None:
    while not _stop.is_set():
        try:
            _heartbeat_once()
        except Exception:
            log.exception("heartbeat failed")
        _stop.wait(settings.heartbeat_seconds)


def _heartbeat_once() -> None:
    with tx() as conn:
        conn.execute("UPDATE step_attempts SET lease_expires_at=? WHERE status='RUNNING'",
                     (iso_plus(settings.lease_seconds),))
    running = query_all(
        """SELECT a.id, a.started_at, s.name, s.id AS step_id, s.task_id, s.config_json,
                  s.timeout_s, t.params_json
           FROM step_attempts a JOIN steps s ON s.id=a.step_id JOIN tasks t ON t.id=s.task_id
           WHERE a.status='RUNNING'"""
    )
    for a in running:
        GPU.touch(a["id"])
        if _seconds_since(a["started_at"]) > float(a["timeout_s"] or 180):
            with _cancel_lock:
                ev = _cancel_events.get(a["id"])
                _cancel_reasons[a["id"]] = "timeout"
            if ev:
                ev.set()


def _reconcile() -> None:
    # 单进程 MVP：启动时所有 RUNNING attempt 都属于上一个已退出进程。
    # 多实例时应改为 lease_expires_at 过期判定（见设计文档 §9.4）。
    orphans = query_all(
        """SELECT a.*, s.status AS step_status, s.task_id, s.name AS step_name,
                  t.cancel_requested, t.status AS task_status, t.trace_id
           FROM step_attempts a JOIN steps s ON s.id=a.step_id JOIN tasks t ON t.id=s.task_id
           WHERE a.status='RUNNING'"""
    )
    for a in orphans:
        with tx() as conn:
            conn.execute(
                """UPDATE step_attempts SET status='ABANDONED', error_class='CRASH',
                       error_code='PROCESS_RESTART', error_message='previous process exited',
                       finished_at=? WHERE id=?""",
                (iso(), a["id"]),
            )
            append_event(conn, a["task_id"], "step.attempt.abandoned", step_id=a["step_id"],
                         attempt_id=a["id"], trace_id=a["trace_id"],
                         payload={"step": a["step_name"], "reason": "process_restart"})
            if a["step_status"] != "RUNNING":
                continue
            if a["cancel_requested"] or a["task_status"] == TaskStatus.CANCELED:
                assert_transition("step", a["step_status"], StepStatus.CANCELED)
                conn.execute("UPDATE steps SET status='CANCELED', updated_at=? WHERE id=?", (iso(), a["step_id"]))
                append_event(conn, a["task_id"], "step.canceled", step_id=a["step_id"],
                             trace_id=a["trace_id"], payload={"reason": "abandoned_cancel"})
            else:
                assert_transition("step", a["step_status"], StepStatus.READY)
                conn.execute("UPDATE steps SET status='READY', updated_at=? WHERE id=?", (iso(), a["step_id"]))
                append_event(conn, a["task_id"], "recovery.resumed", step_id=a["step_id"],
                             attempt_id=a["id"], trace_id=a["trace_id"],
                             payload={"step": a["step_name"], "reason": "process_restart"})
    with tx() as conn:
        conn.execute("UPDATE gpu_leases SET released_at=?, release_reason='process_restart' WHERE released_at IS NULL",
                     (iso(),))
    for p in settings.tmp_dir.glob("*"):
        try:
            p.unlink()
        except OSError:
            pass
    GPU.snapshot()


def _seconds_since(ts: str) -> float:
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - dt).total_seconds()


def _handle_cancels() -> None:
    tasks = query_all("SELECT * FROM tasks WHERE status IN ('PENDING','RUNNING') AND cancel_requested=1")
    for t in tasks:
        # 通知正在执行的 worker
        running = query_all(
            """SELECT a.id AS attempt_id FROM step_attempts a
               JOIN steps s ON s.id=a.step_id
               WHERE s.task_id=? AND a.status='RUNNING'""", (t["id"],)
        )
        for r in running:
            with _cancel_lock:
                _cancel_reasons[r["attempt_id"]] = "user"
                ev = _cancel_events.get(r["attempt_id"])
            if ev:
                ev.set()
        # 取消所有尚未开始的步骤
        pending = query_all(
            """SELECT * FROM steps WHERE task_id=? AND status IN
               ('PENDING','READY','WAITING_RESOURCE','RETRYING')""", (t["id"],)
        )
        for s in pending:
            with tx() as conn:
                row = conn.execute("SELECT status FROM steps WHERE id=?", (s["id"],)).fetchone()
                if row["status"] in {"PENDING", "READY", "WAITING_RESOURCE", "RETRYING"}:
                    assert_transition("step", row["status"], StepStatus.CANCELED)
                    conn.execute("UPDATE steps SET status='CANCELED', updated_at=? WHERE id=?", (iso(), s["id"]))
                    append_event(conn, t["id"], "step.canceled", step_id=s["id"],
                                 payload={"step": s["name"]}, trace_id=t["trace_id"])
        # 没有活动步骤时终结任务
        active = query_one(
            """SELECT COUNT(*) AS n FROM steps WHERE task_id=? AND status IN
               ('PENDING','READY','WAITING_RESOURCE','RUNNING','RETRYING')""", (t["id"],)
        )["n"]
        if int(active) == 0:
            _finish_task(t, TaskStatus.CANCELED, "task.canceled")


def _append_log(path: Path, record: dict) -> None:
    record = {"ts": iso(), **record}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
