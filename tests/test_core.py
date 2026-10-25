from pathlib import Path

import pytest

from app.domain import StepStatus, TaskStatus, assert_transition
from app.errors import ConfigError, TransitionError
from app.fingerprint import step_fingerprint
from app.pipeline import load_pipeline, resolve_effective_config, validate_params


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
