import json

from app.domain import iso, new_id


def append_event(conn, task_id: str, type_: str, *, step_id=None, attempt_id=None,
                 payload=None, trace_id=None, span_id=None) -> int:
    row = conn.execute(
        """INSERT INTO events(task_id, step_id, attempt_id, type, payload_json,
                              trace_id, span_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""",
        (task_id, step_id, attempt_id, type_, json.dumps(payload or {}, ensure_ascii=False),
         trace_id, span_id or new_id(), iso()),
    ).fetchone()
    return int(row["id"])


def list_events(task_id: str, after_id: int = 0, limit: int = 200) -> list[dict]:
    from app.db import query_all
    rows = query_all(
        "SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id LIMIT ?",
        (task_id, after_id, limit),
    )
    for r in rows:
        r["payload"] = json.loads(r.pop("payload_json"))
    return rows
