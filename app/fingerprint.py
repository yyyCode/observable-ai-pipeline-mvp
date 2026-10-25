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
