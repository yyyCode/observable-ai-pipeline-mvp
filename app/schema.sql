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
  definition_json TEXT NOT NULL DEFAULT '{}',
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
