-- Previous forks reminted card row IDs but kept the original tool call ID.
-- Remove only copied image cards without a job of their own.
DELETE FROM messages
WHERE type = 'acp_tool_call'
  AND msg_id GLOB 'async-image:*'
  AND id <> msg_id
  AND CASE WHEN json_valid(content) THEN
        json_type(content, '$.update.raw_output.async_image') = 'object'
        AND json_extract(content, '$.update.tool_call_id') = msg_id
      ELSE 0 END
  AND NOT EXISTS (SELECT 1 FROM async_image_jobs WHERE card_id = messages.id);
