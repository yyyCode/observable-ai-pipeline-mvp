import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from app.errors import TransitionError


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class StepStatus(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    WAITING_RESOURCE = "WAITING_RESOURCE"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELED = "CANCELED"


class AttemptStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"
    CANCELED = "CANCELED"


def _v(x) -> str:
    return x.value if hasattr(x, "value") else str(x)


# 统一用字符串值，便于与 str Enum、数据库字符串比较/哈希。
TASK_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"RUNNING", "CANCELED"},
    "RUNNING": {"SUCCEEDED", "FAILED", "CANCELED"},
    "FAILED": {"RUNNING"},
    "SUCCEEDED": set(),
    "CANCELED": set(),
}

STEP_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"READY", "SKIPPED", "CANCELED"},
    "READY": {"RUNNING", "WAITING_RESOURCE", "CANCELED"},
    "WAITING_RESOURCE": {"READY", "CANCELED"},
    # RUNNING -> READY 只用于启动对账（旧 attempt 已 ABANDONED），不进入重试队列。
    "RUNNING": {"SUCCEEDED", "RETRYING", "FAILED", "CANCELED", "READY"},
    "RETRYING": {"READY", "CANCELED"},
    # 手动重试：失败步骤及其下游重置回 PENDING，重新调度。
    "FAILED": {"READY", "PENDING"},
    "SUCCEEDED": {"PENDING"},
    "SKIPPED": {"PENDING"},
    "CANCELED": {"PENDING"},
}


def assert_transition(kind: str, src, dst) -> None:
    table = TASK_TRANSITIONS if kind == "task" else STEP_TRANSITIONS
    src_v, dst_v = _v(src), _v(dst)
    if dst_v not in table.get(src_v, set()):
        raise TransitionError("INVALID_TRANSITION", f"{kind}: {src_v} -> {dst_v}")


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None = None) -> str:
    return (dt or now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_plus(seconds: int) -> str:
    return iso(now() + timedelta(seconds=seconds))


def terminal_task_statuses() -> set[str]:
    return {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELED}


def active_step_statuses() -> set[str]:
    return {
        StepStatus.PENDING, StepStatus.READY, StepStatus.WAITING_RESOURCE,
        StepStatus.RUNNING, StepStatus.RETRYING,
    }


def backoff_ms(base: int, cap: int, retry_index: int) -> int:
    import random
    return int(min(cap, base * (2 ** retry_index)) * random.uniform(0.8, 1.2))
