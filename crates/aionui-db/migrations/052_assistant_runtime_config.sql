CREATE TABLE IF NOT EXISTS assistant_runtime_configs (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    assistant_id TEXT NOT NULL,
    config_json TEXT NOT NULL,
    PRIMARY KEY (user_id, assistant_id)
);
