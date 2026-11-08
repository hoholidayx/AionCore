ALTER TABLE assistant_definitions
    ADD COLUMN async_image_enabled INTEGER NOT NULL DEFAULT 1 CHECK (async_image_enabled IN (0, 1));
