-- Remove bindings left behind by older conversation deletions.
DELETE FROM acp_session
WHERE NOT EXISTS (
    SELECT 1 FROM conversations WHERE conversations.id = acp_session.conversation_id
);
