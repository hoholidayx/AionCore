-- Older conversations have no trusted creation record.
ALTER TABLE acp_session ADD COLUMN codex_delete_eligible INTEGER NOT NULL DEFAULT 0;
ALTER TABLE acp_session ADD COLUMN codex_delete_thread_id TEXT;
ALTER TABLE acp_session ADD COLUMN codex_delete_home TEXT;
ALTER TABLE acp_session ADD COLUMN codex_delete_phase TEXT NOT NULL DEFAULT 'ready'
    CHECK (codex_delete_phase IN ('ready', 'requested', 'codex_deleted'));
