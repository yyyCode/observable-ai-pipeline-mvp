# 可观测 AI 任务执行管线 MVP 实现计划（1 天压缩版）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在一个工作日内交付可运行、可演示的 AI 任务执行管线 MVP：多步骤 Mock Pipeline、步骤级重试/恢复/结果复用、崩溃续跑、GPU Mock 调度、状态查询 API 与 Dashboard。

**Architecture:** FastAPI 单进程内运行调度器、执行器线程池、心跳/对账与回调发送线程；SQLite（WAL）持久化任务/步骤/尝试/事件；产物按 sha256 存入本地 CAS；前端为单文件原生 HTML/JS，轮询 REST API。

**Tech Stack:** Python 3.11+、FastAPI、uvicorn、pydantic v2、stdlib `sqlite3`、httpx、prometheus_client、pytest、原生 HTML/CSS/JS、Docker。

---

## 0. 1 天排期与取舍

### 0.1 五个任务与时间盒

| 任务 | 内容 | 预计 |
|---|---|---|
| Task 1 | 骨架、配置、schema、状态机、Pipeline 加载、指纹 | 90 min |
| Task 2 | 执行内核：CAS、Mock handler、调度/执行、重试、崩溃恢复、缓存、GPU、日志/指标 | 210 min |
| Task 3 | REST API、SSE、指标、回调 outbox、静态资源挂载 | 90 min |
| Task 4 | Dashboard（任务列表/详情/事件/GPU/总览） | 90 min |
| Task 5 | 演示脚本、Docker、Makefile、README、最终冒烟 | 90 min |
| 缓冲 | 联调、截图、文档打磨 | 60 min |

**目标总时长：约 10 小时（AI 辅助下的 1 个工作日）。**

### 0.2 校验策略（刻意收窄）

- **只有 2 个正式检查点**：
  - **CP1（Task 2 结束）**：不启动前端，用一个真实任务跑通 7 步并落库。
  - **CP2（Task 5 结束）**：`python scripts/demo.py all` 全绿 + 浏览器可展示。
- 不设逐任务 code review、不设覆盖率门槛、不设 CI。
- 只保留 2 个测试文件，覆盖**最容易返工**的纯逻辑：
  - `tests/test_core.py`：状态迁移、Pipeline 校验、参数绑定与指纹。
  - `tests/test_api.py`：创建任务 → 正常完成；注入一次失败 → 重试成功。
- 其余正确性由 `scripts/demo.py` 的 6 个场景覆盖；失败场景不写单测，直接跑演示脚本。

### 0.3 范围取舍

| 级别 | 内容 |
|---|---|
| **P0 必做** | 7 步 Mock Pipeline、三层状态机、重试/超时/取消、心跳租约、启动对账、步骤级结果复用、REST 查询、事件流、GPU Mock 排队/OOM、单文件 Dashboard、6 个演示场景、Docker |
| **P1 顺带做** | SSE 端点、Prometheus 指标、回调 outbox、产物下载、日志 JSONL |
| **P2 明确不做** | 真实 AI/GPU、多 worker、分布式队列、鉴权、OTel、小时级 chunk checkpoint、自动 GC、CI、覆盖率门槛 |

### 0.4 两个硬性原则

1. **每一行关键状态都有 DB 证据**：恢复行为必须能从 `tasks/steps/step_attempts/events` 解释。
2. **演示优先**：任何功能若不能在 Dashboard 或演示脚本中看到，就不算完成。

---

## 1. 文件结构

```text
app/
  __init__.py
  main.py            # FastAPI 入口、lifespan、静态挂载
  config.py          # 环境变量配置与目录
  db.py              # SQLite 连接、事务、查询
  schema.sql         # 建表与索引（幂等）
  errors.py          # 异常分类
  domain.py          # 枚举、状态迁移表、时间工具
  pipeline.py        # Pipeline 加载/校验/参数绑定
  fingerprint.py     # 规范化 JSON 与步骤指纹
  cas.py             # 内容寻址产物写入/读取
  events.py          # 事件追加与查询
  metrics.py         # Prometheus 指标
  handlers.py        # 7 个 Mock handler + HandlerContext
  gpu.py             # GPU Mock 分配器
  executor.py        # 调度器、执行器、心跳、对账
  callback.py        # outbox 回调发送
  api.py             # 全部 REST/SSE 路由
web/
  index.html         # 单文件 Dashboard（内联 CSS/JS）
pipelines/
  animated-asset.v1.json
scripts/
  demo.py            # 6 个演示场景
tests/
  test_core.py
  test_api.py
requirements.txt
Dockerfile
docker-compose.yml
Makefile
README.md
.gitignore
.dockerignore
```

**刻意合并**：前端只有一个文件；API 只有一个模块；不拆 repository/service 层。1 天 MVP 的边界是「可展示」，不是「可长期扩展」。

---

## 2. Task 1：骨架与领域逻辑（90 min）

**Files:** `requirements.txt`、`.gitignore`、`app/__init__.py`、`app/config.py`、`app/schema.sql`、`app/db.py`、`app/errors.py`、`app/domain.py`、`app/pipeline.py`、`app/fingerprint.py`、`pipelines/animated-asset.v1.json`、`tests/test_core.py`

### Step 1: 初始化仓库与依赖

```bash
cd F:/test/demo
git init
python -m venv .venv
# Windows Git Bash:
source .venv/Scripts/activate
pip install fastapi uvicorn pydantic httpx prometheus_client pytest
pip freeze > requirements.txt
```

`.gitignore`：

```gitignore
.venv/
__pycache__/
*.pyc
data/
logs/
.pytest_cache/
```

Expected: `python -c "import fastapi, httpx, prometheus_client"` 无输出。

### Step 2: 配置模块

`app/config.py`：

```python
import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    host: str = os.getenv("APP_HOST", "127.0.0.1")
    port: int = int(os.getenv("APP_PORT", "8000"))
    data_dir: Path = Path(os.getenv("DATA_DIR", "data"))
    log_dir: Path = Path(os.getenv("LOG_DIR", "logs"))
    pipeline_dir: Path = Path(os.getenv("PIPELINE_DIR", "pipelines"))
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "2"))
    lease_seconds: int = int(os.getenv("LEASE_SECONDS", "30"))
    heartbeat_seconds: int = int(os.getenv("HEARTBEAT_SECONDS", "5"))
    gpu_devices: list[dict] = field(default_factory=lambda: json.loads(
        os.getenv("GPU_DEVICES", '[{"id":"gpu0","memory_mb":8192}]')
    ))

    @property
    def db_path(self) -> Path:
        return self.data_dir / "app.db"

    @property
    def cas_dir(self) -> Path:
        return self.data_dir / "cas"

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / "tmp"

    def ensure_dirs(self) -> None:
        for p in (self.data_dir, self.cas_dir, self.tmp_dir, self.log_dir):
            p.mkdir(parents=True, exist_ok=True)


settings = Settings()
```

### Step 3: 建表 SQL（一次写全，后续不再改表）

`app/schema.sql`：

```sql
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  pipeline_version TEXT NOT NULL,
  params_json TEXT NOT NULL,
  status TEXT NOT NULL,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  callback_url TEXT,
  trace_id TEXT NOT NULL,
  error_code TEXT,
  error_message TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS steps (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  name TEXT NOT NULL,
  step_order INTEGER NOT NULL,
  status TEXT NOT NULL,
  depends_on_json TEXT NOT NULL,
  config_json TEXT NOT NULL,
  fingerprint TEXT,
  retry_max INTEGER NOT NULL DEFAULT 3,
  retry_count INTEGER NOT NULL DEFAULT 0,
  gpu_count INTEGER NOT NULL DEFAULT 0,
  gpu_memory_mb INTEGER NOT NULL DEFAULT 0,
  timeout_s INTEGER NOT NULL DEFAULT 180,
  reused INTEGER NOT NULL DEFAULT 0,
  error_class TEXT,
  error_code TEXT,
  error_message TEXT,
  progress INTEGER NOT NULL DEFAULT 0,
  next_retry_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  updated_at TEXT NOT NULL,
  UNIQUE(task_id, name)
);

CREATE TABLE IF NOT EXISTS step_attempts (
  id TEXT PRIMARY KEY,
  step_id TEXT NOT NULL REFERENCES steps(id),
  attempt_no INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_class TEXT,
  error_code TEXT,
  error_message TEXT,
  log_path TEXT,
  metrics_json TEXT,
  worker_id TEXT,
  lease_expires_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  UNIQUE(step_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  step_id TEXT NOT NULL REFERENCES steps(id),
  name TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  mime TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS step_cache (
  fingerprint TEXT PRIMARY KEY,
  artifacts_json TEXT NOT NULL,
  pipeline_name TEXT NOT NULL,
  step_name TEXT NOT NULL,
  hit_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  last_used_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  step_id TEXT,
  attempt_id TEXT,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  trace_id TEXT,
  span_id TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gpu_leases (
  id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  memory_mb INTEGER NOT NULL,
  acquired_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  released_at TEXT,
  release_reason TEXT
);

CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  event TEXT NOT NULL,
  url TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  sent_at TEXT,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_steps_task ON steps(task_id, step_order);
CREATE INDEX IF NOT EXISTS idx_attempts_step ON step_attempts(step_id, attempt_no);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, id);
CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts(task_id);
CREATE INDEX IF NOT EXISTS idx_leases_attempt ON gpu_leases(attempt_id);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, next_attempt_at);
```

### Step 4: DB 访问层

`app/db.py`：

```python
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from app.config import settings

WRITE_LOCK = threading.Lock()
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, check_same_thread=False, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    settings.ensure_dirs()
    conn = connect()
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    finally:
        conn.close()


@contextmanager
def tx():
    with WRITE_LOCK:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()


def query_all(sql: str, params: tuple = ()) -> list[dict]:
    conn = connect()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def query_one(sql: str, params: tuple = ()) -> dict | None:
    rows = query_all(sql, params)
    return rows[0] if rows else None
```

### Step 5: 异常与状态机

`app/errors.py`：

```python
class PipelineError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class ConfigError(PipelineError):
    pass


class RetryableError(PipelineError):
    pass


class FatalError(PipelineError):
    pass


class CanceledError(PipelineError):
    pass


class StepTimeout(RetryableError):
    pass


class TransitionError(PipelineError):
    pass
```

`app/domain.py`：

```python
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


# 统一用字符串值做键，避免 str Enum 与数据库字符串的比较/哈希歧义。
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
    # RUNNING -> READY 只用于启动对账：上个进程遗留的 attempt 已 ABANDONED，步骤重新排队。
    "RUNNING": {"SUCCEEDED", "RETRYING", "FAILED", "CANCELED", "READY"},
    "RETRYING": {"READY", "CANCELED"},
    # 手动重试允许重置失败步骤及其下游：FAILED -> PENDING 或 READY；
    # 已完成/已跳过的下游回到 PENDING 重新调度（缓存仍可命中）。
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
```

### Step 6: Pipeline 加载与参数绑定

`app/pipeline.py`：

```python
import json
import re
from pathlib import Path

from app.errors import ConfigError


def load_pipeline(pipeline_dir: Path, name: str, version: str) -> dict:
    # pipeline 名称来自 API，限制字符集避免路径穿越。
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name) or not re.fullmatch(r"[A-Za-z0-9_.-]+", version):
        raise ConfigError("PIPELINE_INVALID", "invalid pipeline name or version")
    path = pipeline_dir / f"{name}.v{version}.json"
    if not path.is_file():
        raise ConfigError("PIPELINE_NOT_FOUND", f"pipeline {name}@{version} not found")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError("PIPELINE_INVALID", f"invalid pipeline JSON: {exc}") from exc
    if data.get("name") != name or str(data.get("version")) != str(version):
        raise ConfigError("PIPELINE_MISMATCH", f"pipeline file mismatch: {path}")
    steps = data.get("steps") or []
    names = [s["name"] for s in steps]
    if not steps or len(names) != len(set(names)):
        raise ConfigError("PIPELINE_INVALID", "steps empty or duplicate names")

    for idx, step in enumerate(steps):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", str(step.get("name", ""))):
            raise ConfigError("PIPELINE_INVALID", "step name must match [A-Za-z0-9_.-]+")
        step.setdefault("depends_on", [])
        step.setdefault("config", {})
        step.setdefault("params", {})
        step.setdefault("outputs", [])
        step.setdefault("gpu", {"count": 0, "memory_mb": 0})
        step.setdefault("retry", {})
        step.setdefault("timeout_s", 180)
        step["order"] = idx
        for dep in step["depends_on"]:
            if dep not in names:
                raise ConfigError("PIPELINE_INVALID", f"{step['name']} depends on unknown {dep}")
        for key, expr in step["params"].items():
            if not (isinstance(expr, str) and expr.startswith("$task.")):
                raise ConfigError("PIPELINE_INVALID", f"{step['name']}.params.{key} must be $task.x")

    _assert_acyclic(steps)
    return data


def _assert_acyclic(steps: list[dict]) -> None:
    deps = {s["name"]: set(s["depends_on"]) for s in steps}
    ready = [n for n, d in deps.items() if not d]
    seen = 0
    while ready:
        name = ready.pop()
        seen += 1
        for other, ds in deps.items():
            if name in ds:
                ds.remove(name)
                if not ds:
                    ready.append(other)
    if seen != len(steps):
        raise ConfigError("PIPELINE_CYCLE", "pipeline has a dependency cycle")


def resolve_effective_config(step: dict, task_params: dict) -> dict:
    effective = dict(step.get("config") or {})
    for key, expr in (step.get("params") or {}).items():
        param_name = expr.removeprefix("$task.")
        if param_name not in task_params:
            raise ConfigError("MISSING_PARAM", f"{step['name']} requires task param '{param_name}'")
        effective[key] = task_params[param_name]
    return effective


def validate_params(pipeline: dict, task_params: dict) -> None:
    for step in pipeline["steps"]:
        resolve_effective_config(step, task_params)


def validate_gpu(pipeline: dict, devices: list[dict]) -> None:
    max_mb = max((int(d["memory_mb"]) for d in devices), default=0)
    for step in pipeline["steps"]:
        gpu = step.get("gpu") or {}
        count = int(gpu.get("count", 0))
        if count > len(devices):
            raise ConfigError(
                "GPU_TOO_MANY",
                f"{step['name']} requests {count} devices, only {len(devices)} configured",
            )
        if count > 0 and int(gpu.get("memory_mb", 0)) > max_mb:
            raise ConfigError(
                "GPU_TOO_LARGE",
                f"{step['name']} requests {gpu['memory_mb']}MB > device max {max_mb}MB",
            )


def dependents(pipeline: dict, step_name: str) -> set[str]:
    result: set[str] = set()
    changed = True
    while changed:
        changed = False
        for step in pipeline["steps"]:
            if step["name"] in result:
                continue
            if any(dep in result or dep == step_name for dep in step["depends_on"]):
                result.add(step["name"])
                changed = True
    result.discard(step_name)
    return result
```

### Step 7: 指纹

`app/fingerprint.py`：

```python
import hashlib
import json


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def step_fingerprint(
    pipeline_name: str,
    pipeline_version: str,
    step_name: str,
    effective_config: dict,
    input_shas: list[str],
    handler_version: str = "1",
) -> str:
    payload = {
        "pipeline": f"{pipeline_name}@{pipeline_version}",
        "step": step_name,
        "config": effective_config,
        "inputs": sorted(input_shas),
        "handler_version": handler_version,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
```

### Step 8: Pipeline 定义文件

`pipelines/animated-asset.v1.json`：直接使用规格文档 §6.3 的完整 JSON，并确认 `ingest`、`extract_frames`、`style_transfer` 分别有 `params` 绑定 `source`、`target_fps`、`style`。

### Step 9: 核心单测

`tests/test_core.py`：

```python
import pytest

from app.domain import TaskStatus, StepStatus, assert_transition
from app.errors import ConfigError, TransitionError
from app.fingerprint import step_fingerprint
from app.pipeline import load_pipeline, resolve_effective_config, validate_params
from pathlib import Path


def test_task_transition_guard():
    assert_transition("task", TaskStatus.PENDING, TaskStatus.RUNNING)
    with pytest.raises(TransitionError):
        assert_transition("task", TaskStatus.SUCCEEDED, TaskStatus.RUNNING)


def test_step_transition_guard():
    assert_transition("step", StepStatus.RUNNING, StepStatus.RETRYING)
    with pytest.raises(TransitionError):
        assert_transition("step", StepStatus.SKIPPED, StepStatus.RUNNING)


def test_pipeline_loads_and_binds_params():
    p = load_pipeline(Path("pipelines"), "animated-asset", "1")
    assert len(p["steps"]) == 7
    ing = next(s for s in p["steps"] if s["name"] == "ingest")
    eff = resolve_effective_config(ing, {"source": "a.gif", "style": "x", "target_fps": 12})
    assert eff["source"] == "a.gif"


def test_missing_param_rejected():
    p = load_pipeline(Path("pipelines"), "animated-asset", "1")
    with pytest.raises(ConfigError):
        validate_params(p, {"source": "a.gif"})


def test_fingerprint_changes_with_param_but_not_unrelated():
    base = dict(pipeline_name="p", pipeline_version="1", step_name="s", input_shas=["a"])
    f1 = step_fingerprint(**base, effective_config={"x": 1})
    f2 = step_fingerprint(**base, effective_config={"x": 2})
    assert f1 != f2
    assert f1 == step_fingerprint(**base, effective_config={"x": 1})
```

### Step 10: 运行并提交

```bash
pytest tests/test_core.py -q
```

Expected: `5 passed`。

```bash
git add .
git commit -m "feat: foundation - config, schema, state machine, pipeline, fingerprint"
```

---

## 3. Task 2：执行内核（210 min）

**Files:** `app/cas.py`、`app/events.py`、`app/metrics.py`、`app/handlers.py`、`app/gpu.py`、`app/executor.py`

本任务结束时达到 **CP1**：不启动前端，通过 Python 直接插入任务（或临时脚本调用执行器）能跑完 7 步，DB 中状态/产物/事件完整。

### Step 1: CAS 与事件

`app/cas.py`：

```python
import hashlib
import os
import uuid

from app.config import settings


def put_bytes(data: bytes) -> tuple[str, int]:
    sha = hashlib.sha256(data).hexdigest()
    final = settings.cas_dir / sha
    # 同 SHA 但大小不符说明文件损坏；重新写入覆盖，而不是直接复用。
    if final.exists() and final.stat().st_size == len(data):
        return sha, len(data)
    settings.tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = settings.tmp_dir / f"{sha}.{uuid.uuid4().hex}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.replace(tmp, final)
    except OSError:
        # 并发写同一 SHA 时 Windows 可能拒绝覆盖；只要最终文件已存在即可。
        if final.exists():
            tmp.unlink(missing_ok=True)
        else:
            raise
    return sha, len(data)


def path_for(sha: str):
    return settings.cas_dir / sha


def exists_and_valid(sha: str, size: int) -> bool:
    p = path_for(sha)
    return p.exists() and p.stat().st_size == size
```

`app/events.py`：

```python
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
```

### Step 2: 指标

`app/metrics.py`：

```python
from prometheus_client import Counter, Gauge, Histogram

TASKS_TOTAL = Counter("pipeline_tasks_total", "tasks by terminal status", ["status"])
STEPS_TOTAL = Counter("pipeline_steps_total", "steps by status", ["status", "step"])
STEP_DURATION = Histogram("pipeline_step_duration_seconds", "step duration", ["step", "outcome"])
STEP_ATTEMPTS = Counter("pipeline_step_attempts_total", "attempts", ["step", "error_class"])
CACHE_HITS = Counter("pipeline_cache_hits_total", "cache hits", ["step"])
GPU_MEMORY = Gauge("pipeline_gpu_memory_used_mb", "gpu memory used", ["device"])
GPU_QUEUE = Gauge("pipeline_gpu_queue_length", "gpu queue length")
EXECUTOR_UP = Gauge("pipeline_executor_up", "executor alive")
OUTBOX_PENDING = Gauge("pipeline_outbox_pending", "pending callbacks")
```

### Step 3: Mock handlers

`app/handlers.py`：

```python
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
```

### Step 4: GPU 分配器

`app/gpu.py`：

```python
import threading

from app.config import settings
from app.db import query_all, tx
from app.domain import iso, iso_plus, new_id
from app.errors import RetryableError
from app import metrics


class GpuAllocator:
    def __init__(self, devices: list[dict]):
        self.devices = {d["id"]: int(d["memory_mb"]) for d in devices}
        self.lock = threading.Lock()

    def _used(self, device_id: str) -> int:
        rows = query_all(
            "SELECT memory_mb FROM gpu_leases WHERE device_id=? AND released_at IS NULL AND expires_at>?",
            (device_id, iso()),
        )
        return sum(int(r["memory_mb"]) for r in rows)

    def acquire(self, attempt_id: str, count: int, memory_mb: int) -> list[str] | None:
        if count <= 0:
            return []
        with self.lock:
            if memory_mb > max(self.devices.values(), default=0):
                raise RetryableError("CUDA_OOM", f"requested {memory_mb}MB exceeds device capacity")
            chosen = []
            for device_id, total in self.devices.items():
                if len(chosen) >= count:
                    break
                if total - self._used(device_id) >= memory_mb:
                    chosen.append(device_id)
            if len(chosen) < count:
                return None
            with tx() as conn:
                for device_id in chosen:
                    conn.execute(
                        """INSERT INTO gpu_leases(id, device_id, attempt_id, memory_mb,
                                                  acquired_at, expires_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (new_id(), device_id, attempt_id, memory_mb, iso(), iso_plus(settings.lease_seconds)),
                    )
            return chosen

    def release(self, attempt_id: str, reason: str) -> None:
        with tx() as conn:
            conn.execute(
                """UPDATE gpu_leases SET released_at=?, release_reason=?
                   WHERE attempt_id=? AND released_at IS NULL""",
                (iso(), reason, attempt_id),
            )

    def touch(self, attempt_id: str) -> None:
        with tx() as conn:
            conn.execute(
                "UPDATE gpu_leases SET expires_at=? WHERE attempt_id=? AND released_at IS NULL",
                (iso_plus(settings.lease_seconds), attempt_id),
            )

    def snapshot(self) -> dict:
        leases = query_all("SELECT * FROM gpu_leases WHERE released_at IS NULL AND expires_at>?", (iso(),))
        devices = []
        for device_id, total in self.devices.items():
            used = sum(int(l["memory_mb"]) for l in leases if l["device_id"] == device_id)
            devices.append({"id": device_id, "memory_total_mb": total, "memory_used_mb": used,
                            "leases": [l for l in leases if l["device_id"] == device_id]})
            metrics.GPU_MEMORY.labels(device=device_id).set(used)
        queue = query_all(
            """SELECT s.id, s.task_id, s.name, s.gpu_memory_mb, s.updated_at
               FROM steps s JOIN tasks t ON t.id=s.task_id
               WHERE s.status='WAITING_RESOURCE' AND t.status='RUNNING' ORDER BY s.updated_at"""
        )
        metrics.GPU_QUEUE.set(len(queue))
        return {"devices": devices, "queue": queue}


GPU = GpuAllocator(settings.gpu_devices)
```

### Step 5: 执行器骨架（核心循环）

`app/executor.py` 的完整职责与关键实现：

```python
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from app import metrics
from app.cas import exists_and_valid, put_bytes
from app.config import settings
from app.db import query_all, query_one, tx
from app.domain import (
    AttemptStatus, StepStatus, TaskStatus, active_step_statuses, assert_transition,
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
```

`_advance_pending()`：

```python
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
```

`_transition_step()`：

```python
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
```

`_promote_retry()`：

```python
def _promote_retry() -> None:
    rows = query_all(
        """SELECT s.*, t.trace_id FROM steps s JOIN tasks t ON t.id=s.task_id
           WHERE s.status='RETRYING' AND s.next_retry_at IS NOT NULL AND s.next_retry_at<=?""",
        (iso(),),
    )
    for s in rows:
        _transition_step(s, StepStatus.READY, "step.ready", reason="retry_due")
```

`_finalize_tasks()`：

```python
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
    metrics.TASKS_TOTAL.labels(status=status.value if hasattr(status, "value") else status).inc()
    # outbox 入队由 callback.enqueue_terminal 完成，Task 3 接入
```

`_dispatch_ready()`：

```python
def _dispatch_ready() -> None:
    running = query_one("SELECT COUNT(*) AS n FROM step_attempts WHERE status='RUNNING'")["n"]
    slots = settings.max_concurrency - int(running)
    if slots <= 0:
        return
    # 同时处理 READY 与 WAITING_RESOURCE：GPU 释放后在同一次 tick 内唤醒排队步骤。
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
        pipeline = load_pipeline(settings.pipeline_dir, s["pipeline_name"], s["pipeline_version"])
        step_def = next(x for x in pipeline["steps"] if x["name"] == s["name"])
        prepared = _prepare(s, step_def, pipeline, json.loads(s["params_json"]))
        if _try_reuse(s, prepared):
            continue

        attempt_id = new_id()
        device_ids = None
        if int(s["gpu_count"]) > 0:
            try:
                device_ids = GPU.acquire(attempt_id, int(s["gpu_count"]), int(s["gpu_memory_mb"]))
            except RetryableError as exc:
                # 显存请求超过设备容量属于配置错误，创建任务时应已校验；
                # 这里保持 READY，不产生非法状态迁移，下一 tick 重试。
                log.warning("gpu acquire rejected step=%s code=%s", s["name"], exc.code)
                continue
            if device_ids is None:
                if s["status"] != StepStatus.WAITING_RESOURCE:
                    _transition_step(s, StepStatus.WAITING_RESOURCE, "gpu.queued",
                                     memory_mb=s["gpu_memory_mb"])
                continue
            if s["status"] == StepStatus.WAITING_RESOURCE:
                _transition_step(s, StepStatus.READY, "step.ready", reason="gpu_available")

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
        _pool.submit(_run_attempt, s, attempt_id, prepared, step_def, device_ids)
        slots -= 1
```

`_prepare()`、`_try_reuse()`、`_complete_success()`、`_finish_attempt_failure()`、`_run_attempt()`、`_heartbeat_loop()`、`_reconcile()` 的实现：

```python
def _prepare(step: dict, step_def: dict, pipeline: dict, task_params: dict) -> dict:
    effective = resolve_effective_config(step_def, task_params)
    deps = json.loads(step["depends_on_json"])
    input_shas = []
    if deps:
        rows = query_all(
            f"""SELECT a.sha256 FROM artifacts a JOIN steps s ON s.id=a.step_id
                WHERE s.task_id=? AND s.name IN ({','.join('?' * len(deps))})
                ORDER BY a.name""",
            (step["task_id"], *deps),
        )
        input_shas = [r["sha256"] for r in rows]
    fp = step_fingerprint(pipeline["name"], str(pipeline["version"]), step["name"],
                          effective, input_shas, HANDLER_VERSION)
    return {"fingerprint": fp, "effective": effective, "input_shas": input_shas,
            "pipeline_name": pipeline["name"]}


def _try_reuse(step: dict, prepared: dict) -> bool:
    row = query_one("SELECT * FROM step_cache WHERE fingerprint=?", (prepared["fingerprint"],))
    if not row:
        return False
    items = json.loads(row["artifacts_json"])
    if not all(exists_and_valid(i["sha256"], i["size"]) for i in items):
        with tx() as conn:
            conn.execute("DELETE FROM step_cache WHERE fingerprint=?", (prepared["fingerprint"],))
        return False
    with tx() as conn:
        current = conn.execute("SELECT status FROM steps WHERE id=?", (step["id"],)).fetchone()
        current_status = current["status"]
        # 缓存命中仍然走 READY/WAITING_RESOURCE -> RUNNING -> SUCCEEDED 的合法迁移，
        # 只是不执行 handler、不消耗 GPU。排队中的步骤先回到 READY。
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
        # 只允许 RUNNING 步骤走失败/重试迁移；防止取消/完成竞态或预执行失败造成非法状态。
        if step_row["status"] != "RUNNING":
            return
        attempt = conn.execute("SELECT * FROM step_attempts WHERE id=?", (attempt_id,)).fetchone()
        if attempt is None:
            # 防御性补录：正常情况下 RUNNING 步骤必有 attempt 行；
            # 若缺失仍保留一条失败记录，避免历史空白。
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
```

`_heartbeat_loop()`：

```python
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
```

`_reconcile()`：

```python
def _reconcile() -> None:
    # 单进程 MVP：启动时所有 RUNNING attempt 都属于上一个已退出的进程。
    # 多实例部署时才改为按 lease_expires_at 过滤（见规格 §9.4）。
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
        # 通知本任务正在执行的 worker
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
        # 取消所有尚未开始/未终结的步骤
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
        # 没有仍在执行的步骤时立即终结任务；同时覆盖尚未 RUNNING 的 PENDING 任务。
        active = query_one(
            """SELECT COUNT(*) AS n FROM steps WHERE task_id=? AND status IN
               ('PENDING','READY','WAITING_RESOURCE','RUNNING','RETRYING')""", (t["id"],)
        )["n"]
        if int(active) == 0:
            _finish_task(t, TaskStatus.CANCELED, "task.canceled")
```

`_append_log()`：

```python
def _append_log(path: Path, record: dict) -> None:
    record = {"ts": iso(), **record}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
```

**实现要点（必须检查）：**
1. `steps.attempt_no` 不存在；attempt 序号从 `step_attempts` 聚合，`_run_attempt` 通过 `_attempt_no()` 查询。
2. `_try_reuse()` 在 GPU 申请之前执行，缓存命中不占 GPU、不占并发槽。
3. `_finish_attempt_failure()` 中 `retry_count + 1 < retry_max` 表示「本次失败后再给一次机会」；`retry_max` 是总执行次数。
4. 心跳续租所有 RUNNING attempt 与 GPU 租约；超时通过 cancel event 通知 handler。
5. **单进程启动对账**：`_reconcile()` 在启动时把所有 `RUNNING` attempt 视为上个进程遗留的孤儿，标 `ABANDONED`、步骤回 `READY`，并释放全部未释放的 GPU 租约。不能只按租约过期判断，否则 `kill -9` 后立刻重启会因租约仍有 30 秒有效期而永久卡住。
6. `_dispatch_ready()` 同时拾取 `READY` 与 `WAITING_RESOURCE`，GPU 释放后在同一 tick 内唤醒排队步骤；缓存命中的步骤不申请 GPU。
7. `_handle_cancels()` 同时处理 `PENDING` 与 `RUNNING` 任务；取消后若没有仍在执行的步骤，立即终结为 `CANCELED`。
8. `_finish_attempt_failure()` 只处理 `RUNNING` 步骤；其他状态直接返回，避免取消/完成竞态或非法迁移。

### Step 6: CP1 手工验证（不启动前端）

```bash
python - <<'PY'
from app.config import settings
settings.ensure_dirs()
from app.db import init_db, tx
from app.domain import iso, new_id
from app.pipeline import load_pipeline, resolve_effective_config, validate_params
from app.events import append_event
import json

init_db()
p = load_pipeline(settings.pipeline_dir, "animated-asset", "1")
params = {"source": "demo/sample.gif", "style": "watercolor", "target_fps": 12}
validate_params(p, params)
task_id = new_id()
with tx() as c:
    c.execute("""INSERT INTO tasks(id,pipeline_name,pipeline_version,params_json,status,
                 trace_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)""",
              (task_id, "animated-asset", "1", json.dumps(params), "PENDING", new_id(), iso(), iso()))
    for s in p["steps"]:
        c.execute("""INSERT INTO steps(id,task_id,name,step_order,status,depends_on_json,config_json,
                     retry_max,retry_count,gpu_count,gpu_memory_mb,updated_at)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (new_id(), task_id, s["name"], s["order"], "PENDING",
                   json.dumps(s["depends_on"]), json.dumps(resolve_effective_config(s, params)),
                   s["retry"].get("max_attempts", 3), 0,
                   s["gpu"]["count"], s["gpu"]["memory_mb"], iso()))
    append_event(c, task_id, "task.created", payload=params)
print("task_id:", task_id)
PY
```

Expected: 打印 task_id。随后启动执行器：

```bash
python - <<'PY'
import time
from app.config import settings
settings.ensure_dirs()
from app.executor import start, stop
from app.db import query_one
start()
for _ in range(60):
    t = query_one("SELECT status FROM tasks ORDER BY created_at DESC LIMIT 1")
    print(t["status"])
    if t["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
        break
    time.sleep(0.5)
stop()
PY
```

Expected: 依次打印 `PENDING → RUNNING → SUCCEEDED`；DB 中 7 个步骤均为 `SUCCEEDED`，`artifacts` 有记录，`events` 有完整时间线。

### Step 7: 提交

```bash
git add .
git commit -m "feat: execution core - CAS, handlers, scheduler, retry, recovery, cache, GPU"
```

---

## 4. Task 3：API、SSE、回调（90 min）

**Files:** `app/callback.py`、`app/api.py`、`app/main.py`、`tests/test_api.py`

### Step 0: 占位首页（TestClient 需要静态目录存在）

创建 `web/index.html`：

```html
<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>Pipeline</title>
<h1>Dashboard coming in Task 4</h1>
```

> `StaticFiles(directory=...)` 在目录不存在时会在导入阶段报错；Task 3 先放占位，Task 4 覆盖。

### Step 1: 回调 outbox

`app/callback.py`：

```python
import json
import logging
import threading
import time
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
```

### Step 2: 回调接入执行器

在 `app/executor.py` 的 `_finish_task()` **事务内**（`append_event(...)` 之后、`with` 块结束前）加入：

```python
        from app import callback
        callback.enqueue_terminal({**task, "status": status}, conn=conn)
```

> 必须与任务终态同事务写入 outbox，避免「状态已提交、回调丢失」。`callback.enqueue_terminal` 接受可选的 `conn` 正是为此。
> 并确认 `callback.start()` 已在 `app/main.py` 的 lifespan 中调用。

### Step 3: API 路由

`app/api.py` 必须实现以下端点，返回结构直接供 Dashboard 使用：

| 方法/路径 | 关键逻辑 |
|---|---|
| `POST /api/tasks` | 加载 Pipeline、`validate_params`、写 tasks + steps（`config_json` 存有效配置）、事件 `task.created` |
| `GET /api/tasks` | 支持 `status`、`limit`（默认 50）、`cursor`（created_at） |
| `GET /api/tasks/{id}` | 任务 + 步骤列表 + 进度（`succeeded/total*100`） |
| `GET /api/tasks/{id}/steps` | 步骤列表 |
| `GET /api/tasks/{id}/steps/{name}/attempts` | 尝试历史 |
| `GET /api/tasks/{id}/events?after=` | 事件 |
| `GET /api/tasks/{id}/stream` | SSE，支持 `Last-Event-ID` |
| `POST /api/tasks/{id}/cancel` | 置 `cancel_requested=1`；任务已终态返回 409 |
| `POST /api/tasks/{id}/retry` | 仅 FAILED（CANCELED 是终态）；重置失败步骤+下游为 PENDING，任务 RUNNING |
| `POST /api/tasks/{id}/steps/{name}/retry` | 仅 FAILED 步骤；重置该步骤+下游 |
| `GET /api/tasks/{id}/artifacts` | 产物列表 + `download_url` |
| `GET /api/artifacts/{id}` | 元信息 |
| `GET /api/artifacts/{id}/download` | FileResponse |
| `GET /api/gpu` | `GPU.snapshot()` |
| `GET /api/overview` | 任务计数、成功率、平均耗时、缓存命中数 |
| `GET /metrics` | `generate_latest()` |
| `GET /healthz` | DB 可读 + 执行器存活 |

创建任务的核心代码：

```python
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
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
```

> `create_task` 插入任务后，调度器通过 `_promote_tasks()`（`_tick()` 第一步）把 `PENDING` 任务推进为 `RUNNING` 并写 `task.started` 事件。

查询与操作 API（Dashboard、测试和演示脚本依赖这些字段）：

```python
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
```

重试的核心代码：

```python
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
```

SSE：

```python
import asyncio
from fastapi import Request
from fastapi.responses import StreamingResponse
from app.db import query_one
from app.events import list_events


@router.get("/tasks/{task_id}/stream")
async def stream(task_id: str, request: Request, after: int = 0):
    _task_or_404(task_id)
    async def gen():
        last = after or int(request.headers.get("last-event-id", "0") or 0)
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
            else:
                idle += 1
                if idle % 15 == 0:
                    yield ": keepalive\n\n"
                task = await asyncio.to_thread(query_one, "SELECT status FROM tasks WHERE id=?", (task_id,))
                if task and task["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
                    break
                await asyncio.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream")
```

### Step 4: 入口与静态挂载

`app/main.py`：

```python
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app import callback, executor, metrics
from app.api import router
from app.config import settings
from app.db import init_db, query_one
from app.errors import ConfigError, TransitionError


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.ensure_dirs()
    init_db()
    executor.start()
    callback.start()
    yield
    callback.stop()
    executor.stop()


app = FastAPI(title="Observable AI Pipeline MVP", lifespan=lifespan)
app.include_router(router)


@app.exception_handler(ConfigError)
async def handle_config_error(request: Request, exc: ConfigError):
    status = 404 if exc.code == "PIPELINE_NOT_FOUND" else 422
    return JSONResponse(status_code=status,
                        content={"error": {"code": exc.code, "message": exc.message}})


@app.exception_handler(TransitionError)
async def handle_transition_error(request: Request, exc: TransitionError):
    # API 与调度线程竞争时可能撞上状态迁移冲突；调用方应重新查询当前状态。
    return JSONResponse(status_code=409,
                        content={"error": {"code": exc.code, "message": exc.message}})


@app.get("/metrics")
def metrics_endpoint():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
def healthz():
    query_one("SELECT 1")
    return {"ok": True, "executor_up": metrics.EXECUTOR_UP._value.get()}


app.mount("/", StaticFiles(directory=Path(__file__).resolve().parents[1] / "web", html=True), name="web")
```

> 注意：静态挂载必须在 `include_router` 之后；`/api/*`、`/metrics`、`/healthz` 不会被吞掉。`/metrics` 与 `/healthz` 不属于 `/api` 前缀，请在 `app/main.py` 里单独定义（`/metrics` 返回 `generate_latest()`，`/healthz` 检查 `query_one("SELECT 1")` 与 `metrics.EXECUTOR_UP`）。

### Step 5: API 冒烟测试

`tests/test_api.py`：

```python
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
```

Run:

```bash
pytest tests/ -q
```

Expected: `7 passed`（Task 1 的 5 个 + 本文件 2 个）。

### Step 6: 提交

```bash
git add .
git commit -m "feat: REST API, SSE, metrics, callback outbox"
```

---

## 5. Task 4：Dashboard（90 min）

**Files:** `web/index.html`

单文件实现，包含内联 CSS/JS；使用 2 秒轮询，不依赖 SSE（SSE 端点保留给上游）。

### Step 1: 页面结构

```html
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>AI Pipeline MVP</title>
<style>
  :root { --bg:#0f1419; --panel:#171d24; --line:#2a333d; --fg:#e6edf3; --muted:#8b98a5;
          --blue:#3b82f6; --green:#22c55e; --red:#ef4444; --yellow:#eab308; --gray:#6b7280; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.5 ui-monospace, SFMono-Regular, Consolas, monospace;
         background:var(--bg); color:var(--fg); }
  header { display:flex; align-items:center; gap:16px; padding:14px 20px; border-bottom:1px solid var(--line); }
  h1 { font-size:16px; margin:0; }
  nav button, .btn { background:var(--panel); color:var(--fg); border:1px solid var(--line);
                     border-radius:6px; padding:6px 12px; cursor:pointer; }
  nav button.active { border-color:var(--blue); color:var(--blue); }
  main { padding:20px; }
  table { width:100%; border-collapse:collapse; }
  th, td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); }
  tr:hover { background:var(--panel); cursor:pointer; }
  .badge { padding:2px 8px; border-radius:999px; font-size:12px; }
  .badge.SUCCEEDED { background:#12351f; color:var(--green); }
  .badge.RUNNING, .badge.RETRYING { background:#3a2f0b; color:var(--yellow); }
  .badge.FAILED { background:#3a1414; color:var(--red); }
  .badge.PENDING, .badge.READY, .badge.WAITING_RESOURCE { background:#1c2733; color:var(--blue); }
  .badge.SKIPPED, .badge.CANCELED { background:#242a31; color:var(--gray); }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:12px; }
  .steps { display:flex; flex-wrap:wrap; gap:8px; margin:12px 0; }
  .step { min-width:150px; border:1px solid var(--line); border-radius:8px; padding:10px; background:var(--panel); }
  .step .name { font-weight:600; margin-bottom:6px; }
  .muted { color:var(--muted); }
  .bar { height:10px; background:#1c2733; border-radius:999px; overflow:hidden; }
  .bar > i { display:block; height:100%; background:var(--blue); }
  .drawer { margin-top:16px; }
  pre { background:#0b0f14; padding:10px; border-radius:6px; overflow:auto; max-height:260px; }
  .hidden { display:none; }
</style>
</head>
<body>
<header>
  <h1>Observable AI Pipeline</h1>
  <nav>
    <button data-tab="tasks" class="active">任务</button>
    <button data-tab="gpu">GPU</button>
    <button data-tab="overview">总览</button>
  </nav>
  <button class="btn" id="create">新建演示任务</button>
  <span class="muted" id="status"></span>
</header>
<main>
  <section id="tab-tasks">
    <table>
      <thead><tr><th>Task</th><th>状态</th><th>进度</th><th>当前步骤</th><th>耗时</th><th>缓存</th></tr></thead>
      <tbody id="tasks"></tbody>
    </table>
    <div id="detail" class="drawer"></div>
  </section>
  <section id="tab-gpu" class="hidden">
    <div id="gpu" class="grid"></div>
    <h3>排队</h3><table><tbody id="gpu-queue"></tbody></table>
  </section>
  <section id="tab-overview" class="hidden">
    <div id="overview" class="grid"></div>
  </section>
</main>
<script>
const $ = (s) => document.querySelector(s);
let currentTask = null;

async function api(path, opts = {}) {
  const r = await fetch(path, { headers: { "Content-Type": "application/json" }, ...opts });
  if (!r.ok) throw new Error((await r.text()) || r.status);
  return r.json();
}
const fmt = (s) => s ? new Date(s).toLocaleTimeString() : "-";
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const badge = (s) => `<span class="badge ${esc(s)}">${esc(s)}</span>`;

async function refreshTasks() {
  try {
    const rows = await api("/api/tasks?limit=50");
    $("#tasks").innerHTML = rows.map(t => `
      <tr data-id="${t.id}">
        <td>${esc(t.id.slice(0,8))}</td>
        <td>${badge(t.status)}</td>
        <td><div class="bar"><i style="width:${Number(t.progress)||0}%"></i></div></td>
        <td>${esc(t.current_step || "-")}</td>
        <td>${t.duration_s ? t.duration_s.toFixed(1)+"s" : "-"}</td>
        <td>${t.reused_steps ? t.reused_steps+" 步" : "-"}</td>
      </tr>`).join("");
    document.querySelectorAll("#tasks tr").forEach(tr => tr.onclick = () => openTask(tr.dataset.id));
    if (currentTask) openTask(currentTask);
  } catch (e) { $("#status").textContent = e.message; }
}

async function openTask(id) {
  currentTask = id;
  const t = await api(`/api/tasks/${id}`);
  const steps = t.steps.map(s => `
    <div class="step">
      <div class="name">${esc(s.name)} ${s.reused ? '<span class="muted">(cached)</span>' : ""}</div>
      ${badge(s.status)}
      <div class="muted" style="margin-top:6px">attempts: ${Number(s.attempt_count) || 0}</div>
      ${s.error_code ? `<div class="muted">${esc(s.error_code)}</div>` : ""}
      ${s.status === "FAILED" ? `<button class="btn" onclick="retryStep('${id}','${s.name}')">重试</button>` : ""}
    </div>`).join("");
  $("#detail").innerHTML = `
    <div class="card">
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <b>${esc(t.id)}</b> ${badge(t.status)}
        <span class="muted">${esc(t.pipeline)}@${esc(t.version)}</span>
        ${["PENDING","RUNNING"].includes(t.status) ? `<button class="btn" onclick="cancelTask('${id}')">取消</button>` : ""}
        ${t.status === "FAILED" ? `<button class="btn" onclick="retryTask('${id}')">重试任务</button>` : ""}
      </div>
      <div class="steps">${steps}</div>
      <h3>事件</h3><pre>${(t.events||[]).map(e => `${fmt(e.created_at)}  ${esc(e.type)}  ${esc(JSON.stringify(e.payload))}`).join("\n")}</pre>
      <h3>产物</h3>
      <ul>${(t.artifacts||[]).map(a => `<li><a href="${esc(a.download_url)}">${esc(a.step)}/${esc(a.name)}</a> <span class="muted">${esc(a.sha256.slice(0,12))}</span></li>`).join("")}</ul>
    </div>`;
}

async function createTask() {
  const id = crypto.randomUUID().slice(0, 8);
  await api("/api/tasks", { method: "POST", body: JSON.stringify({
    pipeline: "animated-asset", version: "1",
    params: { source: `demo/${id}.gif`, style: "watercolor", target_fps: 12 }
  })});
  refreshTasks();
}
async function cancelTask(id) { await api(`/api/tasks/${id}/cancel`, {method:"POST"}); refreshTasks(); }
async function retryTask(id) { await api(`/api/tasks/${id}/retry`, {method:"POST"}); refreshTasks(); }
async function retryStep(id, name) { await api(`/api/tasks/${id}/steps/${name}/retry`, {method:"POST"}); refreshTasks(); }

async function refreshGpu() {
  const g = await api("/api/gpu");
  $("#gpu").innerHTML = g.devices.map(d => `
    <div class="card"><b>${esc(d.id)}</b><div class="muted">${Number(d.memory_used_mb)} / ${Number(d.memory_total_mb)} MB</div>
    <div class="bar"><i style="width:${Math.min(100, d.memory_used_mb/d.memory_total_mb*100)}%"></i></div></div>`).join("");
  $("#gpu-queue").innerHTML = g.queue.map(q => `<tr><td>${esc(q.task_id.slice(0,8))}</td><td>${esc(q.name)}</td><td>${Number(q.gpu_memory_mb)} MB</td></tr>`).join("");
}
async function refreshOverview() {
  const o = await api("/api/overview");
  $("#overview").innerHTML = Object.entries(o).map(([k,v]) => `<div class="card"><div class="muted">${esc(k)}</div><b>${esc(v)}</b></div>`).join("");
}

document.querySelectorAll("nav button").forEach(b => b.onclick = () => {
  document.querySelectorAll("nav button").forEach(x => x.classList.toggle("active", x === b));
  ["tasks","gpu","overview"].forEach(t => $(`#tab-${t}`).classList.toggle("hidden", t !== b.dataset.tab));
});
$("#create").onclick = createTask;
refreshTasks();
setInterval(refreshTasks, 2000);
setInterval(refreshGpu, 2000);
setInterval(refreshOverview, 2000);
</script>
</body>
</html>
```

### Step 2: 补齐 API 聚合字段

`GET /api/tasks` 每行必须返回：

```text
id, status, progress, current_step, duration_s, reused_steps, created_at
```

`GET /api/tasks/{id}` 必须返回：

```text
id, pipeline, version, status, params, created_at, started_at, finished_at,
progress, steps[], events[], artifacts[]
```

其中：

- 每个 step：`name, status, reused, error_code, error_message, attempt_count, progress`
- 每个 artifact：`id, step, name, sha256, size_bytes, download_url`
- `events` 直接复用 `list_events()`
- `progress = round(succeeded_steps / total_steps * 100)`
- `duration_s = (finished_at or now) - started_at`（秒，保留 1 位小数）
- `reused_steps = count(steps.reused=1)`

实现方式：在 `app/api.py` 用 2–3 条查询聚合，不引入 ORM。

### Step 3: 浏览器冒烟

```bash
uvicorn app.main:app --reload --port 8000
```

打开 `http://127.0.0.1:8000`：

Expected:
1. 点击「新建演示任务」，列表出现任务，状态从 PENDING/RUNNING 变为 SUCCEEDED。
2. 点击任务行，看到 7 个步骤、事件流、产物下载链接。
3. GPU 标签页显示 `gpu0` 显存条；执行期间有占用。
4. 总览显示任务计数与缓存命中数。

### Step 4: 提交

```bash
git add web/index.html app/api.py
git commit -m "feat: single-page dashboard"
```

---

## 6. Task 5：演示、容器化与 README（90 min）

**Files:** `scripts/demo.py`、`Dockerfile`、`.dockerignore`、`docker-compose.yml`、`Makefile`、`README.md`

### Step 1: 演示脚本

`scripts/demo.py`（完整可运行）：

```python
#!/usr/bin/env python3
"""1-day MVP demo: normal, retry, fatal, cache, gpu, cancel, crash."""
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


def start_server(data_dir: Path, extra_env: dict | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    env["DATA_DIR"] = str(data_dir)
    env["LOG_DIR"] = str(data_dir / "logs")
    env.update(extra_env or {})
    cmd = [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(PORT)]
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    for _ in range(150):
        try:
            if httpx.get(f"{BASE}/healthz", timeout=0.5).status_code == 200:
                return proc
        except Exception:
            pass
        time.sleep(0.1)
    proc.kill()
    raise RuntimeError("server failed to start")


def wait_task(client: httpx.Client, task_id: str, timeout: float = 120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = client.get(f"/api/tasks/{task_id}").json()
        if t["status"] in ("SUCCEEDED", "FAILED", "CANCELED"):
            return t
        time.sleep(0.2)
    raise TimeoutError(task_id)


def _src(tag: str) -> str:
    # 每个场景使用唯一 source，避免指纹命中上一场景的缓存而跳过故障注入。
    return f"demo/{tag}-{uuid.uuid4().hex[:8]}.gif"


def create(client: httpx.Client, params: dict | None = None) -> str:
    payload = {
        "pipeline": "animated-asset", "version": "1",
        "params": {"source": _src("task"), "style": "watercolor", "target_fps": 12, **(params or {})},
    }
    r = client.post("/api/tasks", json=payload)
    r.raise_for_status()
    return r.json()["id"]


def scenario_normal(client):
    tid = create(client, {"source": _src("normal")})
    t = wait_task(client, tid)
    assert t["status"] == "SUCCEEDED", t
    assert len(t["steps"]) == 7
    arts = client.get(f"/api/tasks/{tid}/artifacts").json()
    assert len(arts) >= 7
    return f"normal PASS ({len(arts)} artifacts)"


def scenario_retry(client):
    tid = create(client, {"source": _src("retry"),
                          "fail_injections": {"extract_frames": {"retryable_times": 1}}})
    t = wait_task(client, tid)
    assert t["status"] == "SUCCEEDED", t
    attempts = client.get(f"/api/tasks/{tid}/steps/extract_frames/attempts").json()
    assert len(attempts) == 2, attempts
    return "retry PASS (2 attempts)"


def scenario_fatal(client):
    tid = create(client, {"source": _src("fatal"),
                          "fail_injections": {"segment_subject": {"fatal_attempt": 1}}})
    t = wait_task(client, tid)
    assert t["status"] == "FAILED", t
    assert any(s["status"] == "SKIPPED" for s in t["steps"])
    return "fatal PASS (downstream skipped)"


def scenario_cache(client):
    src = _src("cache")
    tid1 = create(client, {"source": src})
    wait_task(client, tid1)
    tid2 = create(client, {"source": src})
    t2 = wait_task(client, tid2)
    assert t2["status"] == "SUCCEEDED", t2
    steps = client.get(f"/api/tasks/{tid2}/steps").json()
    reused = sum(1 for s in steps if s["reused"])
    assert reused >= 6, steps
    return f"cache PASS ({reused}/7 reused)"


def scenario_gpu(client):
    # 两个任务并发争抢单卡 4GB，制造 WAITING_RESOURCE 队列；其中一个注入 OOM。
    slow = {"slow_steps": {"segment_subject": 3, "style_transfer": 3}}
    tid_a = create(client, {"source": _src("gpu-a"), **slow})
    tid_b = create(client, {"source": _src("gpu-b"), **slow,
                            "fail_injections": {"style_transfer": {"retryable_times": 1, "code": "CUDA_OOM"}}})
    queued = False
    for _ in range(200):
        gpu = client.get("/api/gpu").json()
        if gpu["queue"]:
            queued = True
            break
        time.sleep(0.1)
    assert queued, gpu
    ta, tb = wait_task(client, tid_a), wait_task(client, tid_b)
    assert ta["status"] == "SUCCEEDED" and tb["status"] == "SUCCEEDED", (ta, tb)
    attempts = client.get(f"/api/tasks/{tid_b}/steps/style_transfer/attempts").json()
    assert any(a["error_code"] == "CUDA_OOM" for a in attempts), attempts
    return "gpu PASS (queue observed, OOM then success)"


def scenario_cancel(client):
    tid = create(client, {"source": _src("cancel"), "slow_steps": {"style_transfer": 6}})
    for _ in range(200):
        t = client.get(f"/api/tasks/{tid}").json()
        if any(s["name"] == "style_transfer" and s["status"] == "RUNNING" for s in t["steps"]):
            break
        time.sleep(0.1)
    else:
        raise AssertionError("never reached style_transfer")
    client.post(f"/api/tasks/{tid}/cancel")
    t = wait_task(client, tid, timeout=30)
    assert t["status"] == "CANCELED", t
    statuses = {s["name"]: s["status"] for s in t["steps"]}
    assert statuses["ingest"] == "SUCCEEDED", statuses
    assert any(s["status"] == "CANCELED" for s in t["steps"]), statuses
    return "cancel PASS (upstream artifacts preserved)"


def scenario_crash():
    with tempfile.TemporaryDirectory() as td:
        data = Path(td)
        proc = start_server(data)
        try:
            with httpx.Client(base_url=BASE, timeout=10) as client:
                tid = create(client, {"source": _src("crash"), "slow_steps": {"style_transfer": 8}})
                for _ in range(150):
                    t = client.get(f"/api/tasks/{tid}").json()
                    if any(s["name"] == "style_transfer" and s["status"] == "RUNNING" for s in t["steps"]):
                        break
                    time.sleep(0.2)
                else:
                    raise AssertionError("never reached style_transfer")
                proc.kill()
                proc.wait(timeout=10)
            proc = start_server(data)
            with httpx.Client(base_url=BASE, timeout=10) as client:
                t = wait_task(client, tid, timeout=120)
                assert t["status"] == "SUCCEEDED", t
                events = client.get(f"/api/tasks/{tid}/events").json()
                assert any(e["type"] == "recovery.resumed" for e in events), events
            return "crash PASS (resumed after kill -9)"
        finally:
            if proc.poll() is None:
                proc.kill()


def run_all():
    with tempfile.TemporaryDirectory() as td:
        data = Path(td)
        # 单卡 4GB：两个任务并发时 GPU 步骤会真正排队，便于观察。
        proc = start_server(data, {"GPU_DEVICES": '[{"id":"gpu0","memory_mb":4096}]'})
        try:
            with httpx.Client(base_url=BASE, timeout=10) as client:
                results = [
                    scenario_normal(client),
                    scenario_retry(client),
                    scenario_fatal(client),
                    scenario_cache(client),
                    scenario_gpu(client),
                    scenario_cancel(client),
                ]
            # crash 场景会自己再起一个服务；必须先释放当前端口。
            proc.kill()
            proc.wait(timeout=10)
            results.append(scenario_crash())
            for line in results:
                print("✓", line)
        finally:
            if proc.poll() is None:
                proc.kill()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", nargs="?", default="all")
    args = parser.parse_args()
    run_all()
```

### Step 2: 容器化与 Makefile

`Dockerfile`：

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`.dockerignore`：

```text
.venv/
__pycache__/
*.pyc
.pytest_cache/
data/
logs/
.git/
```

`docker-compose.yml`：

```yaml
services:
  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATA_DIR: /app/data
      LOG_DIR: /app/logs
      GPU_DEVICES: '[{"id":"gpu0","memory_mb":8192}]'
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
```

`Makefile`：

```make
.PHONY: install run test demo reset docker
install:
	pip install -r requirements.txt
run:
	uvicorn app.main:app --reload --port 8000
test:
	pytest tests/ -q
demo:
	python scripts/demo.py all
reset:
	rm -rf data logs
docker:
	docker compose up --build
```

### Step 3: README

README 必须包含（对应需求方交付要求）：

1. 一句话简介与截图（跑通后补）。
2. Quick Start：`pip install -r requirements.txt && make run`；或 `docker compose up`。
3. 演示：`make demo`，6 个场景说明与预期输出。
4. 架构图（mermaid 或文本）。
5. 状态机与失败矩阵摘要。
6. 步骤定义与依赖（指向 `pipelines/animated-asset.v1.json`）。
7. 持久化选择与原因（tasks/steps/attempts/events/artifacts/step_cache/gpu_leases/outbox）。
8. 恢复与复用机制（租约、对账、指纹、CAS）。
9. 小时级演进要点（摘要，指向规格文档 §17）。
10. **实现方式与时间规划**（实际 1 天排期与偏差）。
11. **AI coding 工具、token 消耗、token 花得最多的部分**（实现完成后如实填写）。
12. **个人投入时间最多的部分**（不含 LLM 直接产出的部分）。
13. **失败案例分析**：做错/做不了/原因/改进。
14. 已知限制（单机、Mock、索引性能、无鉴权）。

### Step 4: CP2 最终验证

```bash
python scripts/demo.py all
```

Expected:

```text
✓ normal PASS (7+ artifacts)
✓ retry PASS (2 attempts)
✓ fatal PASS (downstream skipped)
✓ cache PASS (7/7 reused)
✓ gpu PASS (OOM then success)
✓ cancel PASS
✓ crash PASS (resumed after kill -9)
```

再用浏览器完成一次手工展示（任务列表 → 详情 → 事件 → 产物 → GPU 面板）。

Docker（可选，若本机可用）：

```bash
docker compose up --build
```

Expected: `http://127.0.0.1:8000` 可访问，创建任务成功。

### Step 5: 提交

```bash
git add .
git commit -m "chore: demo scripts, docker, makefile, README"
```

---

## 7. 关键接口与命名约定（避免实现时命名漂移）

### 7.1 数据库字段

- 任务：`tasks.status`、`cancel_requested`、`trace_id`、`started_at`、`finished_at`。
- 步骤：`steps.step_order`、`status`、`depends_on_json`、`config_json`、`fingerprint`、`retry_max`、`retry_count`、`next_retry_at`、`reused`、`progress`。
- 尝试：`step_attempts.attempt_no`、`status`、`lease_expires_at`、`metrics_json`。
- 事件：`events.type`、`payload_json`、`trace_id`、`span_id`。
- 缓存：`step_cache.fingerprint`、`artifacts_json`、`hit_count`。
- GPU：`gpu_leases.attempt_id`、`device_id`、`memory_mb`、`expires_at`、`released_at`。
- 回调：`outbox.idempotency_key`、`status`、`next_attempt_at`。

### 7.2 模块函数

| 模块 | 函数/对象 | 约定 |
|---|---|---|
| `app.db` | `tx()`、`query_all()`、`query_one()`、`init_db()` | 写操作一律走 `tx()` |
| `app.domain` | `assert_transition(kind, src, dst)`、`iso()`、`iso_plus()`、`new_id()` | 状态变更前必须校验 |
| `app.events` | `append_event(conn, ...)`、`list_events(id, after)` | 与状态变更同事务 |
| `app.pipeline` | `load_pipeline()`、`resolve_effective_config()`、`validate_params()`、`dependents()` | 参数绑定唯一入口 |
| `app.fingerprint` | `step_fingerprint(...)` | 输入 sha 升序 |
| `app.cas` | `put_bytes()`、`path_for()`、`exists_and_valid()` | 原子 rename |
| `app.gpu` | `GPU.acquire()`、`release()`、`touch()`、`snapshot()` | 租约随心跳续期 |
| `app.executor` | `start()`、`stop()`、`_tick()` | 调度循环 0.5s |
| `app.callback` | `enqueue_terminal()`、`start()`、`stop()` | 幂等键 = task:status |
| `app.api` | `router` | 所有路由前缀 `/api` |

### 7.3 事件类型（Dashboard 与演示脚本依赖）

`task.created`、`task.started`、`task.succeeded`、`task.failed`、`task.canceled`、
`task.cancel_requested`、`task.retried`、`step.ready`、`step.skipped`、
`step.attempt.started`、`step.attempt.abandoned`、`step.retrying`、`step.succeeded`、
`step.cache_hit`、`step.failed`、`step.canceled`、`recovery.resumed`、
`gpu.queued`、`callback.sent`、`callback.failed`。

---

## 8. 风险与即时对策（1 天版）

| 风险点 | 触发时机 | 即时对策 |
|---|---|---|
| SQLite 写竞争 | 执行器与 API 同时写 | 全部写走 `tx()` 单写锁；事务内不做慢操作 |
| 调度器重复派发 | 心跳/调度并发 | 调度只在单线程 `_tick()` 中做；派发前事务内复核状态 |
| 缓存误命中 | 参数未绑定 | Task 1 的 `test_missing_param_rejected` 与指纹测试兜底 |
| `kill -9` 后无法续跑 | CP2 crash 场景 | 启动 `_reconcile()`；演示脚本断言语义 |
| Dashboard 字段缺失 | Task 4 联调 | 先实现 API 聚合字段，再写前端 |
| 时间不够 | 第 8 小时 | 砍 P1：先不做 SSE/回调可视化；P0 演示必须全绿 |

### 8.1 时间不够时的裁剪顺序

1. 先砍 `outbox` 回调可视化（保留后端）。
2. 再砍 SSE（Dashboard 已用轮询）。
3. 再砍总览页（保留任务/GPU）。
4. **绝不砍**：正常执行、重试、崩溃恢复、缓存复用、GPU OOM 演示。

---

## 9. 规格覆盖对照（Self-Review）

| 规格条目 | 实现位置 |
|---|---|
| FR-01/02/03/04 任务与 Pipeline | Task 1 + Task 3 创建接口 |
| FR-05/06/07/08/09 执行与状态 | Task 2 执行器、心跳、超时、取消 |
| FR-10/11/12/13 重试与恢复 | Task 2 `_finish_attempt_failure`、`_reconcile` |
| FR-14/15/16 结果复用 | Task 2 `_try_reuse`、`_complete_success`、Task 3 retry |
| FR-17/18/19 查询与可视化 | Task 3 API/SSE、Task 4 Dashboard |
| FR-20/21/22 事件/指标/日志 | Task 2 events/metrics/log、Task 3 `/metrics` |
| FR-23 回调 | Task 3 `callback.py` |
| FR-24~28 GPU Mock | Task 2 `gpu.py`、Task 4 GPU 面板 |
| FR-29/30 演示 | Task 5 `demo.py` |
| NFR-01 一键运行 | Task 5 Makefile/Docker |
| NFR-02 可解释性 | 全部状态落库 + 事件 |
| NFR-05 测试 | 刻意收窄为 2 个测试文件 + 演示脚本（用户要求加速） |
| 规格 §17 小时级演进 | 文档已覆盖，代码不实现（P2） |
| 规格 §21 README 交付要求 | Task 5 Step 3 |

**有意偏离规格之处：**

1. 规格 §22 的 4 天排期压缩为 1 天；测试范围从「单元+集成+E2E+覆盖率」缩为「2 个测试文件 + 演示脚本」。
2. 取消 `force=true` 绕过缓存（MVP 不支持重跑已成功步骤）。
3. 步骤超时已实现，但不单独做演示场景。
4. 小时级 chunk checkpoint、OTel、自动 GC 仅在 README 中说明，不实现。
5. `CANCELED` 任务不支持 retry（按状态机属于终态）；只允许 `FAILED` 任务重试。
6. 单进程启动对账把所有 `RUNNING` attempt 视为孤儿；多实例语义（按租约过期）只在规格与 README 中说明。
7. 缓存复用校验「文件存在 + 大小」，未在全量复用路径重算 SHA256；大文件哈希校验留作 P2。

**占位符检查：** 本计划不含未完成标记；所有代码片段均为可落地实现或明确的改动点。

---

## 10. 执行交接

计划已保存到 `docs/superpowers/plans/2026-10-25-observable-ai-pipeline-mvp.md`。

两种执行方式：

1. **Subagent-Driven（推荐）**：每个 Task 派一个新 subagent，任务间只做一次快速检查；适合 1 天冲刺。
2. **Inline Execution**：在当前会话按 Task 顺序执行，仅在 CP1/CP2 停下来确认。

选择后即可开始。建议先执行 Task 1，随后直接连续执行到 CP1，中途不做额外评审。
