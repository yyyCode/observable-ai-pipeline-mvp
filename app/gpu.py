import threading

from app import metrics
from app.config import settings
from app.db import query_all, tx
from app.domain import iso, iso_plus, new_id
from app.errors import RetryableError


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
