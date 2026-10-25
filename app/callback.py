import json
import logging
import threading
from datetime import datetime, timezone

import httpx

from app import metrics
from app.db import query_all, query_one, tx
from app.domain import iso, iso_plus, new_id

log = logging.getLogger(__name__)
_stop = threading.Event()


def enqueue_terminal(task: dict, conn=None) -> None:
    if not task.get("callback_url"):
        return
    status = task["status"].value if hasattr(task["status"], "value") else str(task["status"])
    key = f"{task['id']}:{status}"
    params = (new_id(), task["id"], f"task.{status.lower()}", task["callback_url"],
              key, json.dumps({"task_id": task["id"], "status": status}), iso(), iso())
    sql = """INSERT OR IGNORE INTO outbox(id, task_id, event, url, idempotency_key,
                 payload_json, status, attempts, next_attempt_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)"""
    if conn is not None:
        conn.execute(sql, params)
    else:
        with tx() as c:
            c.execute(sql, params)


def start() -> None:
    _stop.clear()
    threading.Thread(target=_loop, daemon=True, name="outbox").start()


def stop() -> None:
    _stop.set()


def _loop() -> None:
    while not _stop.is_set():
        try:
            _drain_once()
        except Exception:
            log.exception("outbox drain failed")
        _stop.wait(2)


def _drain_once() -> None:
    rows = query_all(
        "SELECT * FROM outbox WHERE status='pending' AND next_attempt_at<=? ORDER BY created_at LIMIT 20",
        (iso(),),
    )
    pending = query_one("SELECT COUNT(*) AS n FROM outbox WHERE status='pending'")
    metrics.OUTBOX_PENDING.set(int(pending["n"]) if pending else 0)
    for row in rows:
        try:
            resp = httpx.post(row["url"], json=json.loads(row["payload_json"]),
                              headers={"Idempotency-Key": row["idempotency_key"]}, timeout=5)
            resp.raise_for_status()
            with tx() as conn:
                conn.execute("UPDATE outbox SET status='sent', sent_at=?, attempts=attempts+1 WHERE id=?",
                             (iso(), row["id"]))
                conn.execute("INSERT INTO events(task_id, type, payload_json, created_at) VALUES (?,?,?,?)",
                             (row["task_id"], "callback.sent", json.dumps({"url": row["url"]}), iso()))
        except Exception as exc:
            attempts = int(row["attempts"]) + 1
            delay = min(60, 2 ** attempts)
            with tx() as conn:
                conn.execute(
                    """UPDATE outbox SET attempts=?, next_attempt_at=?, last_error=?,
                           status=CASE WHEN ?>=10 THEN 'failed' ELSE 'pending' END WHERE id=?""",
                    (attempts, iso_plus(delay), str(exc), attempts, row["id"]),
                )
                conn.execute("INSERT INTO events(task_id, type, payload_json, created_at) VALUES (?,?,?,?)",
                             (row["task_id"], "callback.failed", json.dumps({"error": str(exc)}), iso()))
    pending = query_one("SELECT COUNT(*) AS n FROM outbox WHERE status='pending'")
    metrics.OUTBOX_PENDING.set(int(pending["n"]) if pending else 0)
