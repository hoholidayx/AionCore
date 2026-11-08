ALTER TABLE mcp_servers ADD COLUMN purpose TEXT NOT NULL DEFAULT 'general'
    CHECK (purpose IN ('general', 'async_image') AND
           (purpose = 'general' OR (transport_type = 'http' AND builtin = 0)));

ALTER TABLE mcp_servers ADD COLUMN is_default INTEGER NOT NULL DEFAULT 0
    CHECK (is_default IN (0, 1) AND (is_default = 0 OR purpose = 'async_image'));

CREATE UNIQUE INDEX mcp_servers_async_image_default
    ON mcp_servers (user_id)
    WHERE is_default = 1 AND deleted_at IS NULL;
