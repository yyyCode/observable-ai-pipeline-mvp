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
    names = [s.get("name") for s in steps]
    if not steps or len(names) != len(set(names)):
        raise ConfigError("PIPELINE_INVALID", "steps empty or duplicate names")

    for idx, step in enumerate(steps):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", str(step.get("name", ""))):
            raise ConfigError("PIPELINE_INVALID", "step name must match [A-Za-z0-9_.-]+")
        step["depends_on"] = step.get("depends_on") or []
        step["config"] = step.get("config") or {}
        step["params"] = step.get("params") or {}
        step["outputs"] = step.get("outputs") or []
        step["gpu"] = step.get("gpu") or {"count": 0, "memory_mb": 0}
        step["retry"] = step.get("retry") or {}
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
