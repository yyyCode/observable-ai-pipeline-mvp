import os
import shutil
import tempfile
from pathlib import Path

# 必须在 app.config 导入前设置，避免测试写入仓库内的 data/ 与 logs/。
_TEST_ROOT = Path(tempfile.mkdtemp(prefix="pipeline-mvp-tests-"))
os.environ["DATA_DIR"] = str(_TEST_ROOT / "data")
os.environ["LOG_DIR"] = str(_TEST_ROOT / "logs")
os.environ["PIPELINE_DIR"] = str(Path(__file__).resolve().parents[1] / "pipelines")
os.environ["MAX_CONCURRENCY"] = "2"


def pytest_sessionfinish(session, exitstatus):
    # SQLite 连接均为短生命周期；TestClient 关闭后可以安全清理。
    shutil.rmtree(_TEST_ROOT, ignore_errors=True)
