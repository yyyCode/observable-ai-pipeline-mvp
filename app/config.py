import json
import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:  # pragma: no cover - dotenv 是可选便利依赖
    pass


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
