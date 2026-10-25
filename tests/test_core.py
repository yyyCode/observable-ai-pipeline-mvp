import threading
from pathlib import Path

import pytest

from app.domain import StepStatus, TaskStatus, assert_transition
from app.errors import ConfigError, FatalError, TransitionError
from app.executor import validate_outputs
from app.fingerprint import step_fingerprint
from app.handlers import (
    HANDLERS, HandlerContext, ingest, extract_frames, segment_subject,
    style_transfer, upscale, compose_spritesheet, publish,
)
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


def test_mock_outputs_carry_source_provenance():
    """防止 Mock 产物跨输入相同，导致内容指纹误命中缓存。"""
    source = "fixture/unique-source.gif"
    slow = {name: 0 for name in ("ingest", "extract_frames", "segment_subject",
                                 "style_transfer", "upscale", "compose_spritesheet", "publish")}
    ctx = HandlerContext(
        step_name="test", config={"source": source},
        params={"source": source, "style": "watercolor", "slow_steps": slow},
        attempt_no=1, cancel_event=threading.Event(), log=lambda record: None,
    )
    funcs = [ingest, extract_frames, segment_subject, style_transfer,
             upscale, compose_spritesheet, publish]
    for func in funcs:
        outputs = func(ctx)
        payload = b"".join(outputs.values())
        assert source.encode() in payload, func.__name__
    # handler 注册表覆盖流水线中引用的全部实现。
    assert set(HANDLERS) == {f"mock.{f.__name__}" for f in funcs}


def test_handlers_match_pipeline_output_contract():
    source = "fixture/contract.gif"
    params = {"source": source, "style": "watercolor", "target_fps": 12,
              "slow_steps": {name: 0 for name in (
                  "ingest", "extract_frames", "segment_subject", "style_transfer",
                  "upscale", "compose_spritesheet", "publish")}}
    pipeline = load_pipeline(Path("pipelines"), "animated-asset", "1")
    for step in pipeline["steps"]:
        ctx = HandlerContext(
            step_name=step["name"], config=resolve_effective_config(step, params),
            params=params, attempt_no=1, cancel_event=threading.Event(),
            log=lambda record: None,
        )
        outputs = HANDLERS[step["handler"]](ctx)
        validate_outputs(step, outputs)
        assert source.encode() in b"".join(outputs.values()), step["name"]

    with pytest.raises(FatalError):
        validate_outputs({"outputs": ["expected.txt"]}, {"other.txt": b"x"})
    with pytest.raises(FatalError):
        validate_outputs({"outputs": ["../escape.txt"]}, {"../escape.txt": b"x"})
    with pytest.raises(FatalError):
        validate_outputs({"outputs": ["a.txt"]}, {"a.txt": "not-bytes"})
