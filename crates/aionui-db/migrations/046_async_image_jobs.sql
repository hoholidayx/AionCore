CREATE TABLE IF NOT EXISTS async_image_jobs (
    task_id               TEXT PRIMARY KEY NOT NULL,
    user_id               TEXT NOT NULL,
    conversation_id       TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    card_id               TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    turn_id               TEXT NOT NULL,
    tool_call_id          TEXT NOT NULL,
    server_id             TEXT NOT NULL,
    endpoint_fingerprint  TEXT NOT NULL,
    contract_version      TEXT NOT NULL,
    request_body          TEXT,
    state                 TEXT NOT NULL CHECK(state IN ('pending', 'submitting', 'polling', 'completed', 'failed')),
    result_png            BLOB,
    next_check_at         INTEGER NOT NULL,
    retry_started_at      INTEGER,
    claim_token           TEXT,
    claim_until           INTEGER,
    created_at            INTEGER NOT NULL,
    UNIQUE(conversation_id, turn_id, tool_call_id)
);

CREATE INDEX IF NOT EXISTS idx_async_image_jobs_due ON async_image_jobs(next_check_at, created_at)
    WHERE state IN ('pending', 'submitting', 'polling');
