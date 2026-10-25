import hashlib
import os
import uuid

from app.config import settings


def put_bytes(data: bytes) -> tuple[str, int]:
    sha = hashlib.sha256(data).hexdigest()
    final = settings.cas_dir / sha
    # 同 SHA 但大小不符说明文件损坏，重写覆盖；大小一致直接复用。
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
        # 并发写同一 SHA 时 Windows 可能拒绝替换；只要目标文件已存在即可。
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
