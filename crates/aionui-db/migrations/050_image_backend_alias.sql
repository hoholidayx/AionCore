ALTER TABLE mcp_servers ADD COLUMN backend_alias TEXT
    CHECK (backend_alias IS NULL OR (
        purpose = 'async_image' AND length(backend_alias) BETWEEN 1 AND 32
        AND backend_alias NOT GLOB '*[^a-z0-9-]*'
        AND backend_alias NOT GLOB '-*' AND backend_alias NOT GLOB '*-'
        AND backend_alias NOT GLOB '*--*'
    ));

CREATE UNIQUE INDEX IF NOT EXISTS mcp_servers_image_backend_selector
    ON mcp_servers (user_id, COALESCE(backend_alias, name))
    WHERE purpose = 'async_image' AND deleted_at IS NULL;
