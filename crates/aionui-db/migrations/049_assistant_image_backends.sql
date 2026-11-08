-- NULL inherits the account catalog. An explicit empty array disables image generation.
ALTER TABLE assistant_definitions ADD COLUMN async_image_backend_ids TEXT;
