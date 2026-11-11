CREATE UNIQUE INDEX IF NOT EXISTS conversations_dots_default_owner
    ON conversations(user_id)
    WHERE json_valid(extra) AND json_extract(extra, '$.dots_default') = 1;

CREATE TABLE IF NOT EXISTS dots_bridge_requests (
    message_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    binding TEXT NOT NULL,
    text TEXT NOT NULL,
    files TEXT NOT NULL,
    artifact_ids TEXT,
    request_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'replied', 'failed', 'cancelled')),
    UNIQUE(binding, request_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS dots_bridge_one_pending_turn
    ON dots_bridge_requests(conversation_id, binding) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS dots_bridge_pending_binding
    ON dots_bridge_requests(binding, state);

CREATE TABLE IF NOT EXISTS dots_bridge_attachments (
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    binding TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    PRIMARY KEY (conversation_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS dots_bridge_consumers (
    binding TEXT PRIMARY KEY,
    start_sequence INTEGER NOT NULL
);
