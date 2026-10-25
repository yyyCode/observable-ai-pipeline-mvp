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
