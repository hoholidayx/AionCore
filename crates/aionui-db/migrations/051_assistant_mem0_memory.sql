ALTER TABLE assistant_overlays ADD COLUMN mem0_memory_enabled INTEGER NOT NULL DEFAULT 0
    CHECK (mem0_memory_enabled IN (0, 1));
